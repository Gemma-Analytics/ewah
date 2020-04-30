from ewah.dwhooks.base_dwhook import EWAHBaseDWHook
from ewah.constants import EWAHConstants as EC

import os
import csv
import snowflake.connector
from tempfile import NamedTemporaryFile
from airflow.utils.file import TemporaryDirectory
from airflow.models import BaseOperator

class SnowflakeOperator(BaseOperator):
    "Operate to execute SQL on Snowflake"

    def __init__(
        self,
        sql,
        snowflake_conn_id,
        database,
        params=None,
    *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.conn_id = snowflake_conn_id
        self.database = database
        self.params = params

    def execute(self, context):
        hook = EWAHDWHookSnowflake(
            conn_id=self.conn_id,
            database=self.database,
        )
        self.log.info('execute: '+self.sql)
        hook.execute(self.sql, commit=True, params=self.params)
        hook.close()

class EWAHDWHookSnowflake(EWAHBaseDWHook):

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

    def __init__(self, *args, database=None, **kwargs):
        self.database = database
        super().__init__(EC.DWH_ENGINE_SNOWFLAKE, *args, **kwargs)


    def _create_conn(self, database=None):
        creds = self.credentials
        extra = creds.extra_dejson
        extra['password'] = creds.password
        if creds.host:
            extra['account'] = creds.host
        if creds.login:
            extra['user'] = creds.login
        if database or self.database:
            extra['database'] = database or self.database

        return snowflake.connector.connect(**extra)

    def _create_or_update_table(
        self,
        data,
        table_name,
        schema_name,
        database_name,
        columns_definition,
        columns_partial_query,
        update_on_columns,
        drop_and_replace,
        logging_function,
    ):
        logging_function('Preparing DWH Tables...')
        new_table_name = table_name + '_new'
        self.execute(
            sql="""CREATE OR REPLACE TABLE
                    "{database_name}"."{schema_name}"."{table_name}"
                    ({columns});
            """.format(**{
                'database_name': database_name,
                'schema_name': schema_name,
                'table_name': new_table_name,
                'columns': columns_partial_query,
            }),
            commit=False,
        )

        list_of_columns = self.execute_and_return_result(
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

        self.log.info('Writing data to a temporary .csv file')
        with TemporaryDirectory(prefix='uploadtosnowflake') as tmp_dir:
            with NamedTemporaryFile(
                dir=tmp_dir,
                prefix=new_table_name,
                suffix='.csv',
            ) as datafile:
                file_name = os.path.abspath(datafile.name)
                with open(file_name, mode='w') as csv_file:
                    csvwriter = csv.writer(
                        csv_file,
                        delimiter=',',
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
                sql_upload = '''
                    USE DATABASE "{2}";
                    USE SCHEMA "{3}";
                    DROP FILE FORMAT IF EXISTS {1}_format;
                    CREATE OR REPLACE FILE FORMAT {1}_format
                        type = 'CSV'
                        field_delimiter = ','
                        field_optionally_enclosed_by = '"'
                    ;
                    DROP STAGE IF EXISTS {1}_stage;
                    CREATE OR REPLACE STAGE {1}_stage
                        file_format = {1}_format;
                    PUT file://{0} @{1}_stage;
                    COPY INTO "{2}"."{3}"."{1}" FROM '@{1}_stage/{4}.gz';
                '''.format(
                    file_name,
                    new_table_name,
                    database_name,
                    schema_name,
                    file_name[file_name.rfind('/')+1:],
                )
                self.log.info('Uploading data to Snowflake...')
                self.execute(sql_upload)

        if drop_and_replace or (not self.test_if_table_exists(
            table_name=table_name,
            schema_name=schema_name,
            database_name=database_name,
        )):
            sql_final = '''
                USE SCHEMA "{0}"."{1}";
                DROP TABLE IF EXISTS "{0}"."{1}"."{2}" CASCADE;
                ALTER TABLE "{0}"."{1}"."{3}" RENAME TO "{2}";
            '''.format(
                database_name,
                schema_name,
                table_name,
                new_table_name,
            )
        else:
            update_set_cols = []
            for col in columns_definition.keys():
                if not (col in update_on_columns):
                    update_set_cols += [col]

            sql_final = '''
                USE SCHEMA "{0}"."{1}";
                MERGE INTO "{0}"."{1}"."{2}" AS a
                    USING "{0}"."{1}"."{3}" AS b
                    ON {4}
                    WHEN MATCHED THEN UPDATE
                    SET {5}
                    WHEN NOT MATCHED THEN INSERT
                    ({6}) VALUES ({7})
                    ;
                DROP TABLE "{0}"."{1}"."{3}" CASCADE;
            '''.format(
                database_name,
                schema_name,
                table_name,
                new_table_name,
                ' AND '.join([
                    'a."{0}" = b."{0}"'.format(col) for col in update_on_columns
                ]),
                ', '.join([
                    'a."{0}" = b."{0}"'.format(col) for col in update_set_cols
                ]),
                '"'+'", "'.join(list(columns_definition.keys()))+'"',
                ', '.join([
                    'b."{0}"'.format(col) \
                    for col in list(columns_definition.keys())
                ]),
            )

        self.log.info('Final Step: Merging data')
        self.execute(sql_final)

    def commit(self):
        self.cur.execute('COMMIT;')
        self.cur.execute('BEGIN;')

    def rollback(self):
        self.cur.execute('ROLLBACK;')
        self.cur.execute('BEGIN;')

    def test_if_table_exists(
        self,
        table_name,
        schema_name,
        database_name=None,
    ):
        self.execute('USE DATABASE {0}'.format(
            database_name or self.database_name,
        ))
        return 0 < len(self.execute_and_return_result(
            """
            SELECT * FROM "{0}".information_schema.tables
            WHERE table_schema LIKE '{1}'
            AND table_name LIKE '{2}'
            """.format(
                database_name or self.database_name,
                schema_name,
                table_name,
            )
        ))
