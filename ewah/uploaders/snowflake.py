"""
The Snowflake Uploader is currently not available due to questionable design decisions
by the Snowflake Python SDK maintainers.
"""

from ewah.uploaders.base import EWAHBaseUploader
from ewah.constants import EWAHConstants as EC

import os
import csv
import pickle

import snowflake.connector
from tempfile import NamedTemporaryFile, TemporaryDirectory
from airflow.models import BaseOperator


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
    _QUERY_SCHEMA_CHANGES_DROP_COLUMN = """
        ALTER TABLE "{database_name}"."{schema_name}"."{table_name}"
        DROP COLUMN IF EXISTS "{column_name}" CASCADE;
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

    def __init__(self, *args, database=None, **kwargs):
        self.database = database  # TODO: can I remove this??
        super().__init__(EC.DWH_ENGINE_SNOWFLAKE, *args, **kwargs)

    def commit(self):
        # Execute delayed upload
        if hasattr(self, "_tempdir"):
            # Upload data from pickled files in the correct order
            for filename in [
                str(i + 1)
                for i in range(max([int(x) for x in os.listdir(self.tempdir)]))
            ]:
                self.log.info("Unpickling and uploading data from {0}".format(filename))
                with open(self.tempdir + os.sep + filename, "rb") as pickle_file:
                    kwargs = pickle.load(pickle_file)
                    if (kwargs["upload_call_count"] > 1) or (
                        not (kwargs["load_strategy"] == EC.LS_INSERT_REPLACE)
                    ):
                        super().detect_and_apply_schema_changes(
                            new_schema_name=(
                                kwargs["schema_name"] + kwargs["schema_suffix"]
                            ),
                            new_table_name=kwargs["table_name"],
                            new_columns_dictionary=kwargs["columns_definition"],
                            database=kwargs.get("database_name", None),
                            drop_missing_columns=False,
                            commit=False,
                        )
                    self._create_or_update_table_from_pickle(**kwargs)

            self._tempdir.cleanup()
            del self._tempdir
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

    def detect_and_apply_schema_changes(self, *args, **kwargs):
        # Overwrite parent's function: delay execution until the commit command!
        return (["(Execution delayed)"], ["(Execution delayed)"])

    def _create_or_update_table(self, **kwargs):
        """Instead of loading the data straight away, store the data as pickle and
        load it into the DWH when
        """
        upload_call_count = kwargs["upload_call_count"]
        self.log.info(
            "Pickling data as {0} to load at the end...".format(str(upload_call_count))
        )
        with open(self.tempdir + os.sep + str(upload_call_count), "wb") as f:
            pickle.dump(kwargs, f)

    def _create_or_update_table_from_pickle(
        self,
        data,
        table_name,
        schema_name,
        schema_suffix,
        columns_definition,
        columns_partial_query,
        update_on_columns,
        load_strategy,
        upload_call_count,
        database_name=None,
        pk_columns=None,
    ):
        database_name = database_name or self.dwh_hook.conn.database
        pk_columns = pk_columns or []
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
                columns=columns_partial_query,
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
                            [datum.get(col) for col in list_of_columns],
                        )

                # now stage and copy into snowflake!
                sql_upload = """
                    USE DATABASE "{2}";
                    USE SCHEMA "{3}";
                    DROP FILE FORMAT IF EXISTS {1}_format;
                    CREATE FILE FORMAT {1}_format
                        type = 'CSV'
                        field_delimiter = ','
                        field_optionally_enclosed_by = '"'
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
            if pk_columns:
                sql_final += """
                    ALTER TABLE "{0}"."{1}"."{2}"
                    ADD PRIMARY KEY ("{3}");
                """.format(
                    database_name,
                    schema_name,
                    table_name,
                    '","'.join(pk_columns),
                )
        else:
            update_set_cols = []
            for col in columns_definition.keys():
                if not (col in update_on_columns):
                    update_set_cols += [col]

            sql_final = """
                USE SCHEMA "{0}"."{1}";
                MERGE INTO "{0}"."{1}"."{2}" AS a
                    USING "{0}"."{1}"."{3}" AS b
                    ON {4}
                    WHEN MATCHED THEN UPDATE
                        SET {5}
                    WHEN NOT MATCHED THEN INSERT
                                ({6})
                        VALUES  ({7})
                    ;
                DROP TABLE "{0}"."{1}"."{3}" CASCADE;
            """.format(
                database_name,
                schema_name,
                table_name,
                new_table_name,
                " AND ".join(
                    ['a."{0}" = b."{0}"'.format(col) for col in update_on_columns]
                )
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
                database_name or self.database or self.dwh_hook.conn.database,
            )
        )
        return 0 < len(
            self.dwh_hook.execute_and_return_result(
                """
            SELECT * FROM "{0}".information_schema.tables
            WHERE table_schema LIKE '{1}'
            AND table_name LIKE '{2}'
            """.format(
                    database_name or self.database or self.dwh_hook.conn.database,
                    schema_name,
                    table_name,
                )
            )
        )

    def get_max_value_of_column(
        self, column_name, table_name, schema_name, database_name=None
    ):
        self.dwh_hook.execute(
            "USE DATABASE {0}".format(
                database_name or self.database or self.dwh_hook.conn.database,
            )
        )
        return self.dwh_hook.execute_and_return_result(
            sql='SELECT MAX("{0}") FROM {1}'.format(
                column_name,
                f'"{schema_name}"."{table_name}"',
            ),
            return_dict=False,
        )[0][0]
