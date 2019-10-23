from ewah.ewah_dwhooks.ewah_base_dwhook import EWAHBaseDWHook
from ewah.constants import EWAHConstants as EC

import snowflake.connector

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

    def __init__(self, database=None, *args, **kwargs):
        if database:
            self.database = database
        super().__init__(EC.DWH_ENGINE_SNOWFLAKE, *args, **kwargs)


    def _create_conn(self, database=None):
        creds = self.credentials
        extra = creds.extra_dejson
        extra[password] = creds.password
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
        # database_name,
        columns_definition,
        columns_partial_query,
        update_on_columns,
        drop_and_replace,
        logging_function,
    ):
        logging('Whoop!')

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
                table_name,
                schema_name,
                database_name or self.database_name,
            )
        ))
