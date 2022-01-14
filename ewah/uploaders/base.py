from airflow.utils.log.logging_mixin import LoggingMixin

from ewah.hooks.base import EWAHBaseHook
from ewah.constants import EWAHConstants as EC

import math
import pickle
import os

from copy import deepcopy
from tempfile import TemporaryDirectory
from typing import Optional, Type, Union, Dict, List, Any


class EWAHBaseUploader(LoggingMixin):
    """Base class for all EWAH uploader classes aka Uploaders.

    Uploaders are classes that can receive data in a standardized format and upload
    it to a target data storage. Each Uploader is uploads data to one type of target.
    For instance, the EWAHPostgresUploader loads data to PostgreSQL databases.

    The base class contains the basic methods shared by all Uploaders for the purpose of
    uploading data. Each target shall have a subclass derived from this class.
    """

    upload_call_count = 0

    def __init__(
        self,
        dwh_engine: str,
        dwh_conn: Type[EWAHBaseHook],
        load_strategy: str,
        cleaner: "EWAHCleaner",
        table_name: str,
        schema_name: str,
        schema_suffix: str = "_next",
        database_name: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        use_temp_pickling: bool = False,
        pickling_upload_chunk_size: int = 100000,
        pickle_compression: Optional[str] = None,
    ) -> None:
        assert pickle_compression is None or pickle_compression in (
            "gzip",
            "bz2",
            "lzma",
        )

        if use_temp_pickling:
            # Set a function to use for temporary pickle files
            if pickle_compression is None:
                self.pickle_file_open = open
            else:
                self.pickle_file_open = __import__(pickle_compression).open

            # Prepare file used for temporary data pickling
            self.temp_pickle_folder = TemporaryDirectory()
            self.temp_file_name = (
                self.temp_pickle_folder.name + os.sep + "temp_pickle_file"
            )
            self.temp_pickle_file = self.pickle_file_open(self.temp_file_name, "wb")

        super().__init__()
        self.dwh_engine = dwh_engine
        self.dwh_conn = dwh_conn
        self.dwh_hook = dwh_conn.get_hook()
        self.load_strategy = load_strategy
        self.cleaner = cleaner
        self.table_name = table_name
        self.schema_name = schema_name
        self.schema_suffix = schema_suffix
        self.database_name = database_name
        self.primary_key = primary_key
        self.use_temp_pickling = use_temp_pickling
        self.pickling_upload_chunk_size = pickling_upload_chunk_size

    @classmethod
    def get_cleaner_callables(cls):
        # overwrite me for cleaner callables that are always called
        return []

    @classmethod
    def get_schema_tasks(cls, *args, **kwargs):
        """
        This classmethod needs to be overwritten by a child class and return
        a tuple of two tasks, the kickoff and final tasks for the data loading DAGs.

        The kickoff task is supposed to create a new, empty schema to use during data
        loading.

        The final task is supposed to replace the old with the new schema e.g. by
        removing the old schema and renaming the new schema.
        """
        raise Exception("Not implemented!")

    @property
    def columns_definition(self):
        return self.cleaner.get_columns_definition(dwh_engine=self.dwh_engine)

    def _get_column_type(self, column_definition: dict) -> str:
        """Return the column type of the field. Returns the DWH engine specific default
        if there is none.

        :param column_definition: Dictionary containing details on this column,
            including optionally the column type.
        """
        return column_definition.get(
            EC.QBC_FIELD_TYPE,
            EC.QBC_TYPE_MAPPING[self.dwh_engine].get(str),
        )

    def upload_data(
        self,
        data: List[Dict[str, any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:

        data = self.cleaner.clean_rows(rows=data, metadata=metadata)

        if self.primary_key:
            for pk_name in self.primary_key:
                if not pk_name in self.columns_definition.keys():
                    raise Exception(
                        ("Column {0} does not exist but is " + "expected!").format(
                            pk_name
                        )
                    )

        if self.use_temp_pickling:
            # earmark for later upload
            self._upload_via_pickling(data=data)
        else:
            # upload straightaway
            self._upload_data(data)

    def _upload_data(self, data=None):
        if not data:
            self.log.info("No data to upload!")
            return
        self.upload_call_count += 1
        self.log.info(
            "Chunk {1}: Uploading {0} rows of data.".format(
                str(len(data)),
                str(self.upload_call_count),
            )
        )

        if (self.upload_call_count > 1) or (
            not (self.load_strategy == EC.LS_INSERT_REPLACE)
        ):
            self.log.info("Checking for, and applying schema changes.")
            self.log.info(
                "Added fields:\n\t{0}\n".format(
                    "\n\t".join(
                        self.detect_and_apply_schema_changes()
                        or ["No fields were added."]
                    )
                )
            )

        self.create_or_update_table(
            data=data,
            upload_call_count=self.upload_call_count,
            primary_key=self.primary_key,
            commit=False,  # See note below for reason
        )
        """ Note on committing changes:
            The hook used for data uploading is created at the beginning of the
            execute function and automatically committed and closed at the end.
            DO NOT commit in this function, as multiple uploads may be required,
            and any intermediate commit may be subsequently followed by an
            error, which would then result in incomplete data committed.
        """

    def _upload_via_pickling(self, data: Union[dict, List[dict]]):
        """Call this function to earmark a dictionary for later upload."""
        assert self.use_temp_pickling, "Can only call function if using temp pickling!"
        if isinstance(data, dict):
            pickle.dump(data, self.temp_pickle_file)
        elif isinstance(data, list):
            self.log.info(
                "Pickling {0} rows of data for later upload...".format(len(data))
            )
            for row in data:
                assert isinstance(
                    row, dict
                ), "Invalid data format for function! Must be dict or list of dicts!"
                pickle.dump(row, self.temp_pickle_file)
        else:
            raise Exception(
                "Invalid data format for function! Must be dict or list of dicts!"
            )

    def _upload_from_pickle(self):
        """Call this function to upload previously pickled data."""
        assert self.use_temp_pickling
        assert hasattr(self, "temp_pickle_file")
        self.temp_pickle_file.close()
        self.temp_pickle_file = self.pickle_file_open(self.temp_file_name, "rb")
        keep_unpickling = True
        while keep_unpickling:
            raw_data = []
            for _ in range(self.pickling_upload_chunk_size):
                try:
                    raw_data.append(pickle.load(self.temp_pickle_file))
                except EOFError:
                    # all done
                    keep_unpickling = False
                    break
            self._upload_data(raw_data)
        # re-open a new pickle file for new data
        self.temp_pickle_file.close()
        self.temp_pickle_file = self.pickle_file_open(self.temp_file_name, "wb")

    def finalize_upload(self):
        if self.use_temp_pickling:
            self._upload_from_pickle()

    def close(self):
        self.dwh_hook.close()

    def copy_table(self) -> None:
        """Copy the existing version of the table, including all data, if it exists."""
        test_kwargs = {"table_name": self.table_name, "schema_name": self.schema_name}
        if self.database_name:
            test_kwargs["database_name"] = self.database_name
        if self.test_if_table_exists(**test_kwargs):
            if self.database_name:
                kwargs = {"database_name": self.database_name}
            else:
                kwargs = {}
            self.dwh_hook.execute(
                sql=self._COPY_TABLE.format(
                    old_schema=self.schema_name,
                    old_table=self.table_name,
                    new_schema=self.schema_name + self.schema_suffix,
                    new_table=self.table_name,
                    **kwargs,
                ),
                commit=False,
            )

    def detect_and_apply_schema_changes(self):
        # Note: Don't commit any changes!
        params = {
            "schema_name": self.schema_name + self.schema_suffix,
            "table_name": self.table_name,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            params["database_name"] = self.database_name
        elif self.dwh_engine == EC.DWH_ENGINE_BIGQUERY:
            params["project_id"] = self.database_name
        if not self.test_if_table_exists(**params):
            # Table did not previously exist, so there is nothing to do
            return

        list_of_old_columns = [
            col[0].strip()
            for col in self.dwh_hook.execute_and_return_result(
                sql=self._QUERY_SCHEMA_CHANGES_COLUMNS.format(**params),
                params=params,
                return_dict=False,
            )
        ]
        list_of_columns = [
            col_name.strip() for col_name in list(self.columns_definition.keys())
        ]

        new_columns = []
        for column in list_of_columns:
            if not (column in list_of_old_columns):
                new_columns += [column]
                add_params = deepcopy(params)
                add_params["column_name"] = column
                add_params["column_type"] = self._get_column_type(
                    self.columns_definition[column]
                )
                self.dwh_hook.execute(
                    sql=self._QUERY_SCHEMA_CHANGES_ADD_COLUMN.format(
                        **add_params,
                    ),
                    commit=False,
                )

        return new_columns

    def create_or_update_table(
        self,
        data,
        upload_call_count,
        primary_key=None,
        commit=False,
    ):
        # check this again with Snowflake!!
        database_name = self.database_name

        self.log.info(
            "Uploading {0} rows of data...".format(
                str(len(data)),
            )
        )

        kwargs = {
            "data": data,
            "table_name": self.table_name,
            "schema_name": self.schema_name,
            "schema_suffix": self.schema_suffix,
            "columns_definition": self.columns_definition,
            "load_strategy": self.load_strategy,
            "upload_call_count": upload_call_count,
            "primary_key": primary_key,
        }
        if database_name:
            kwargs["database_name"] = database_name

        self._create_or_update_table(**kwargs)

        if commit:
            self.dwh_hook.commit()
