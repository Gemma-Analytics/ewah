from airflow.utils.log.logging_mixin import LoggingMixin

from ewah.hooks.base import EWAHBaseHook
from ewah.constants import EWAHConstants as EC

import math

from copy import deepcopy
from typing import Optional, Type, Union, Dict


class EWAHBaseUploader(LoggingMixin):
    """Base class for all EWAH uploader classes aka Uploaders.

    Uploaders are classes that can receive data in a standardized format and upload
    it to a target data storage. Each Uploader is uploads data to one type of target.
    For instance, the EWAHPostgresUploader loads data to PostgreSQL databases.

    The base class contains the basic methods shared by all Uploaders for the purpose of
    uploading data. Each target shall have a subclass derived from this class.
    """

    def __init__(
        self,
        dwh_engine: str,
        dwh_conn: Type[EWAHBaseHook],
        load_strategy: str,
        table_name: str,
        schema_name: str,
        schema_suffix: str = "_next",
        database_name: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.dwh_engine = dwh_engine
        self.dwh_conn = dwh_conn
        self.dwh_hook = dwh_conn.get_hook()
        self.load_strategy = load_strategy
        self.table_name = table_name
        self.schema_name = schema_name
        self.schema_suffix = schema_suffix
        self.database_name = database_name

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

    def detect_and_apply_schema_changes(
        self,
        new_columns_dictionary,
        drop_missing_columns=False,
        database=None,
    ):
        # Note: Don't commit any changes!
        params = {
            "schema_name": self.schema_name + self.schema_suffix,
            "table_name": self.table_name,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            params["database_name"] = self.database_name
        if not self.test_if_table_exists(**params):
            # Table did not previously exist, so there is nothing to do
            return ([], [])

        list_of_old_columns = [
            col[0].strip()
            for col in self.dwh_hook.execute_and_return_result(
                sql=self._QUERY_SCHEMA_CHANGES_COLUMNS.format(**params),
                params=params,
                return_dict=False,
            )
        ]
        list_of_columns = [
            col_name.strip() for col_name in list(new_columns_dictionary.keys())
        ]

        dropped_columns = []
        if drop_missing_columns:
            for old_column in list_of_old_columns:
                if not (old_column in list_of_columns):
                    drop_params = deepcopy(params)
                    drop_params["column_name"] = old_column
                    dropped_columns += [old_column]
                    self.dwh_hook.execute(
                        sql=self._QUERY_SCHEMA_CHANGES_DROP_COLUMN.format(
                            **drop_params,
                        ),
                        commit=False,
                    )

        new_columns = []
        for column in list_of_columns:
            if not (column in list_of_old_columns):
                new_columns += [column]
                add_params = deepcopy(params)
                add_params["column_name"] = column
                add_params["column_type"] = self._get_column_type(
                    new_columns_dictionary[column]
                )
                self.dwh_hook.execute(
                    sql=self._QUERY_SCHEMA_CHANGES_ADD_COLUMN.format(
                        **add_params,
                    ),
                    commit=False,
                )

        return (new_columns, dropped_columns)

    def create_or_update_table(
        self,
        data,
        upload_call_count,
        columns_definition,
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
            "columns_definition": columns_definition,
            "load_strategy": self.load_strategy,
            "upload_call_count": upload_call_count,
            "primary_key": primary_key,
        }
        if database_name:
            kwargs["database_name"] = database_name

        self._create_or_update_table(**kwargs)

        if commit:
            self.dwh_hook.commit()
