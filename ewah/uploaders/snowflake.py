"""
The Snowflake Uploader is currently not available due to questionable design decisions
by the Snowflake Python SDK maintainers.
"""

from ewah.uploaders.base import EWAHBaseUploader
from ewah.constants import EWAHConstants as EC
from ewah.hooks.base import EWAHBaseHook

import os
import sys
import csv
import pickle
import pytz

import snowflake.connector
from tempfile import NamedTemporaryFile, TemporaryDirectory
from airflow.models import BaseOperator
from copy import deepcopy
from datetime import datetime


class SnowflakeOperator(BaseOperator):
    "Operate to execute SQL on Snowflake"

    def __init__(self, sql, snowflake_conn_id, database, params=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = snowflake_conn_id
        self.database = database
        # an attribute called "params" of an operator leads to an airflow error
        # if that attribute is None due to default templating of "task.params"!
        self._params = params

    def execute(self, context):
        from ewah.hooks.snowflake import EWAHSnowflakeHook

        hook = EWAHSnowflakeHook(
            conn_id=self.conn_id,
            database=self.database,
        )
        hook.execute(self.sql, commit=True, params=self._params)
        hook.commit()
        hook.close()


class EWAHSnowflakeUploader(EWAHBaseUploader):
    _QUERY_SCHEMA_CHANGES_COLUMNS = """
        SELECT
            column_name
        FROM "{database_name}".information_schema.columns
        WHERE table_catalog = %(database_name)s
            AND table_schema = %(schema_name)s
            AND table_name = %(table_name)s
        ORDER BY ordinal_position ASC;
    """
    _QUERY_SCHEMA_CHANGES_ADD_COLUMN = """
        ALTER TABLE "{database_name}"."{schema_name}"."{table_name}"
        ADD COLUMN "{column_name}" {column_type};
    """
    _QUERY_TABLE = """
        SELECT * FROM "{database_name}"."{schema_name}"."{table_name}"
    """

    _COPY_TABLE = """
        CREATE OR REPLACE TABLE "{database_name}"."{new_schema}"."{new_table}"
            CLONE "{database_name}"."{old_schema}"."{old_table}";
    """

    def __init__(self, *args, **kwargs):
        super().__init__(EC.DWH_ENGINE_SNOWFLAKE, *args, **kwargs)
        # Snowflake database name may be set in the connection
        self.database_name = self.database_name or self.dwh_hook.conn.database

    @classmethod
    def get_cleaner_callables(cls):
        def add_timezone(row):
            # Snowflake uses Pacific Time as default time zone if it receives
            # a timestamp without a time zone. Overwrite this default by adding
            # UTC as time zone to every datetime that doesn't have a time zone.
            for key, value in row.items():
                if isinstance(value, datetime) and not value.tzinfo:
                    # add UTC as timezone if there is no timezone for the datetime yet
                    row[key] = value.replace(tzinfo=pytz.utc)
            return row

        return [add_timezone]

    @classmethod
    def get_schema_tasks(
        cls,
        dag,
        dwh_engine,
        dwh_conn_id,
        target_schema_name,
        target_schema_suffix="_next",
        target_database_name=None,
        read_right_users=None,  # Only for PostgreSQL
        **additional_task_args,
    ):
        target_database_name = target_database_name or (
            EWAHBaseHook.get_connection(dwh_conn_id).database
        )
        sql_kickoff = """
            DROP SCHEMA IF EXISTS
                "{database}"."{schema_name}{schema_suffix}" CASCADE;
            CREATE SCHEMA "{database}"."{schema_name}{schema_suffix}";
        """.format(
            database=target_database_name,
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )
        sql_final = """
            DROP SCHEMA IF EXISTS "{database}"."{schema_name}" CASCADE;
            ALTER SCHEMA "{database}"."{schema_name}{schema_suffix}"
                RENAME TO "{schema_name}";
        """.format(
            database=target_database_name,
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )

        def execute_snowflake(sql, conn_id, **kwargs):
            hook = EWAHBaseHook.get_hook_from_conn_id(conn_id)
            hook.execute(sql)
            hook.commit()
            hook.close()

        task_1_args = deepcopy(additional_task_args)
        task_2_args = deepcopy(additional_task_args)
        task_1_args.update(
            {
                "task_id": "kickoff",
                "sql": sql_kickoff,
                "snowflake_conn_id": dwh_conn_id,
                "database": target_database_name,
                "dag": dag,
            }
        )
        task_2_args.update(
            {
                "task_id": "final",
                "sql": sql_final,
                "snowflake_conn_id": dwh_conn_id,
                "database": target_database_name,
                "dag": dag,
            }
        )
        return (SnowflakeOperator(**task_1_args), SnowflakeOperator(**task_2_args))

    def commit(self):
        self.dwh_hook.commit()

    def rollback(self):
        self.dwh_hook.rollback()

    def close(self):
        self.dwh_hook.close()

    @property
    def tempdir(self):
        if not hasattr(self, "_tempdir"):
            self._tempdir = TemporaryDirectory(prefix="uploadtosnowflake")
        return self._tempdir.name

    def _create_or_update_table(
        self,
        data,
        table_name,
        schema_name,
        schema_suffix,
        columns_definition,
        load_strategy,
        upload_call_count,
        database_name=None,
        primary_key=None,
    ):
        primary_key = primary_key or []  # must be empty list, but don't init in kwargs
        database_name = database_name or self.dwh_hook.conn.database
        self.log.info("Preparing DWH Tables...")
        schema_name += schema_suffix
        new_table_name = table_name + "_new"
        self.dwh_hook.execute(
            sql="""CREATE OR REPLACE TABLE
                    "{database_name}"."{schema_name}"."{table_name}"
                    ({columns});
            """.format(
                database_name=database_name,
                schema_name=schema_name,
                table_name=new_table_name,
                columns=",\n\t".join(
                    [
                        '"{0}"\t{1}'.format(col, self._get_column_type(defi))
                        for col, defi in columns_definition.items()
                    ]
                ),
            ),
            commit=False,
        )

        list_of_columns = self.dwh_hook.execute_and_return_result(
            sql="""SELECT
                column_name
                FROM "{0}".information_schema.columns
                WHERE table_catalog = '{0}'
                AND table_schema = '{1}'
                AND table_name = '{2}'
                ORDER BY ordinal_position ASC
            """.format(
                database_name,
                schema_name,
                new_table_name,
            ),
        )
        list_of_columns = [col[0].strip() for col in list_of_columns]
        python_fix_required = sys.version_info.minor < 10  # See below for reason

        self.log.info("Writing data to a temporary .csv file")
        with TemporaryDirectory(prefix="uploadtosnowflake") as tmp_dir:
            with NamedTemporaryFile(
                dir=tmp_dir,
                prefix=new_table_name,
                suffix=".csv",
            ) as datafile:
                file_name = os.path.abspath(datafile.name)
                with open(file_name, mode="w") as csv_file:
                    csvwriter = csv.writer(
                        csv_file,
                        delimiter=",",
                        quotechar='"',
                        quoting=csv.QUOTE_MINIMAL,
                    )

                    for _ in range(len(data)):
                        datum = data.pop(0)
                        # Make sure order of csv is the same as order of columns
                        csvwriter.writerow(
                            [
                                # csv has a bug, which is fixed in Python 3.10,
                                # which leads to the escape character itself not
                                # being escaped - Snowflake uploads will fail
                                # if it is not escaped, hence strings with backslashes
                                # need double-slashes to "manually" escape it.
                                # Hotfix can be removed when upgrading to Python >= 3.10
                                datum[col].replace("\\", "\\\\")
                                if python_fix_required
                                and isinstance(datum.get(col), str)
                                else datum.get(col)
                                for col in list_of_columns
                            ],
                        )

                # now stage and copy into snowflake!
                sql_upload = """
                    USE DATABASE "{2}";
                    USE SCHEMA "{3}";
                    DROP FILE FORMAT IF EXISTS {1}_format;
                    CREATE FILE FORMAT {1}_format
                        TYPE = 'CSV'
                        FIELD_DELIMITER = ','
                        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                        ESCAPE = '\\\\'
                    ;
                    DROP STAGE IF EXISTS {1}_stage;
                    CREATE STAGE {1}_stage
                        file_format = {1}_format;
                    PUT file://{0} @{1}_stage AUTO_COMPRESS = TRUE OVERWRITE = TRUE;
                    COPY INTO "{2}"."{3}"."{1}" FROM '@{1}_stage/{4}.gz';
                """.format(
                    file_name,
                    new_table_name,
                    database_name,
                    schema_name,
                    file_name[file_name.rfind("/") + 1 :],
                )
                self.log.info("Uploading data to Snowflake...")
                self.dwh_hook.execute(sql_upload)

        if (load_strategy == EC.LS_INSERT_REPLACE and upload_call_count == 1) or (
            not self.test_if_table_exists(
                table_name=table_name,
                schema_name=schema_name,
                database_name=database_name,
            )
        ):
            sql_final = """
                USE SCHEMA "{0}"."{1}";
                DROP TABLE IF EXISTS "{0}"."{1}"."{2}" CASCADE;
                ALTER TABLE "{0}"."{1}"."{3}" RENAME TO "{2}";
            """.format(
                database_name,
                schema_name,
                table_name,
                new_table_name,
            )
            if primary_key:
                sql_final += """
                    ALTER TABLE "{0}"."{1}"."{2}"
                    ADD PRIMARY KEY ("{3}");
                """.format(
                    database_name,
                    schema_name,
                    table_name,
                    '","'.join(primary_key),
                )
        else:
            update_set_cols = []
            if load_strategy == EC.LS_INSERT_ADD:
                # Even if set, ignore primary key during insert!
                primary_key = []

            for col in columns_definition.keys():
                if not (col in primary_key):
                    update_set_cols += [col]

            sql_final = """
                USE SCHEMA "{0}"."{1}";
                MERGE INTO "{0}"."{1}"."{2}" AS a
                    USING "{0}"."{1}"."{3}" AS b
                    ON {4}
                    WHEN MATCHED THEN UPDATE
                        SET {5}
                    WHEN NOT MATCHED THEN
                        INSERT  ({6})
                        VALUES  ({7})
                    ;
                DROP TABLE "{0}"."{1}"."{3}" CASCADE;
            """.format(
                database_name,
                schema_name,
                table_name,
                new_table_name,
                " AND ".join(['a."{0}" = b."{0}"'.format(col) for col in primary_key])
                or "FALSE",
                ", ".join(['a."{0}" = b."{0}"'.format(col) for col in update_set_cols]),
                '"' + '", "'.join(list(columns_definition.keys())) + '"',
                ", ".join(
                    ['b."{0}"'.format(col) for col in list(columns_definition.keys())]
                ),
            )

        self.log.info("Final Step: Merging data")
        self.dwh_hook.execute(sql_final)

    def test_if_table_exists(
        self,
        table_name,
        schema_name,
        database_name=None,
    ):
        self.dwh_hook.execute(
            "USE DATABASE {0}".format(
                database_name or self.database_name,
            )
        )
        return 0 < len(
            self.dwh_hook.execute_and_return_result(
                """
            SELECT * FROM "{0}".information_schema.tables
            WHERE table_schema LIKE '{1}'
            AND table_name LIKE '{2}'
            """.format(
                    database_name or self.database_name,
                    schema_name,
                    table_name,
                )
            )
        )

    def get_max_value_of_column(self, column_name):
        self.dwh_hook.execute("USE DATABASE {0}".format(self.database_name))
        return self.dwh_hook.execute_and_return_result(
            sql='SELECT MAX("{0}") FROM {1}'.format(
                column_name,
                f'"{self.schema_name}{self.schema_suffix}"."{self.table_name}"',
            ),
            return_dict=False,
        )[0][0]
