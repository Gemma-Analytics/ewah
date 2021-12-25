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
        self, dwh_engine: str, dwh_conn: Type[EWAHBaseHook], *args, **kwarg
    ) -> None:
        super().__init__(*args, **kwarg)
        self.dwh_engine = dwh_engine
        self.dwh_conn = dwh_conn
        self.dwh_hook = dwh_conn.get_hook()

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

    def copy_table(
        self,
        old_schema: str,
        old_table: str,
        new_schema: str,
        new_table: str,
        **kwargs: Optional[Dict[str, str]],  # e.g. database_name
    ) -> None:
        """Copy a table, including all data.

        :param old_schema: Schema name of the source table.
        :param old_table: Table name of the source table.
        :param new_schema: Schema name of the newly created table.
        :param new_table: Table name of the newly created table.

        :Keyword Arguments:
            * Any argument that may be given as format keyword argument to format the
                ``_COPY_TABLE`` default SQL, e.g. ``database_name`` for Snowflake
        """
        test_kwargs = {"table_name": old_table, "schema_name": old_schema}
        test_kwargs.update(
            {key: value for (key, value) in kwargs.items() if not value is None}
        )
        if self.test_if_table_exists(**test_kwargs):
            try:  # refactor ASAP - snowflake bugfix
                kwargs["database_name"] = (
                    kwargs.get("database_name", None) or self.dwh_conn.database
                )
            except:
                pass
            self.dwh_hook.execute(
                sql=self._COPY_TABLE.format(
                    old_schema=old_schema,
                    old_table=old_table,
                    new_schema=new_schema,
                    new_table=new_table,
                    **kwargs,
                ),
                commit=False,
            )

    def detect_and_apply_schema_changes(
        self,
        new_schema_name,
        new_table_name,
        new_columns_dictionary,
        drop_missing_columns=False,
        database=None,
        commit=False,
    ):
        params = {
            "schema_name": new_schema_name,
            "table_name": new_table_name,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            database = database or self.database or self.dwh_hook.conn.database
            params["database_name"] = database
        if not self.test_if_table_exists(**params):
            return ([], [])  # Table did not previously exist, so there is nothing to do

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

        if commit:
            self.dwh_hook.commit()

        return (new_columns, dropped_columns)

    def create_or_update_table(
        self,
        data,
        load_strategy,
        upload_call_count,
        columns_definition,
        table_name,
        schema_name,
        schema_suffix,
        database_name=None,
        primary_key=None,
        commit=False,
    ):
        # check this again with Snowflake!!
        database_name = database_name or getattr(self, "database", None)

        self.log.info(
            "Uploading {0} rows of data...".format(
                str(len(data)),
            )
        )

        sql_part_columns = ",\n\t".join(
            [
                '"{0}"\t{1}'.format(col, self._get_column_type(defi))
                for col, defi in columns_definition.items()
            ]
        )

        kwargs = {
            "data": data,
            "table_name": table_name,
            "schema_name": schema_name,
            "schema_suffix": schema_suffix,
            "columns_definition": columns_definition,
            "columns_partial_query": sql_part_columns,
            "load_strategy": load_strategy,
            "upload_call_count": upload_call_count,
            "primary_key": primary_key,
        }
        if database_name:
            kwargs["database_name"] = database_name

        self._create_or_update_table(**kwargs)

        if commit:
            self.dwh_hook.commit()
