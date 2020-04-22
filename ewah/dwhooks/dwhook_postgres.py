from ewah.dwhooks.base_dwhook import EWAHBaseDWHook
from ewah.constants import EWAHConstants as EC

from psycopg2.extras import execute_values
from psycopg2 import connect as pg_connect

class EWAHDWHookPostgres(EWAHBaseDWHook):

    _QUERY_SCHEMA_CHANGES_COLUMNS = """
        SELECT
        	f.attname AS "name"
        FROM pg_attribute f
        	JOIN pg_class cl ON cl.OID = f.attrelid
        	LEFT JOIN pg_namespace n ON n.OID = cl.relnamespace
        WHERE cl.relkind = 'r'::CHAR
        	AND n.nspname = %(schema_name)s
        	AND cl.relname = %(table_name)s
        	AND f.attnum > 0;
    """
    _QUERY_SCHEMA_CHANGES_DROP_COLUMN = """
        ALTER TABLE "{schema_name}"."{table_name}"
        DROP COLUMN IF EXISTS "{column_name}" CASCADE;
    """
    _QUERY_SCHEMA_CHANGES_ADD_COLUMN = """
        ALTER TABLE "{schema_name}"."{table_name}"
        ADD COLUMN "{column_name}" {column_type};
    """
    _QUERY_TABLE = 'SELECT * FROM "{schema_name}"."{table_name}"'

    def __init__(self, *args, **kwargs):
        super().__init__(EC.DWH_ENGINE_POSTGRES, *args, **kwargs)

    def _create_conn(self):
        return pg_connect(
            "dbname='{0}' user='{1}' host='{2}' password='{3}' port='{4}'"
            .format(
                self.credentials.schema,
                self.credentials.login,
                self.credentials.host,
                self.credentials.password,
                self.credentials.port,
            )
        )

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
        upload_chunking=100000,
    ):
        logging_function('Preparing DWH Tables...')
        if drop_and_replace or (not self.test_if_table_exists(
            table_name=table_name,
            schema_name=schema_name,
        )):
            self.execute(
                sql="""
                    DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;
                    CREATE TABLE "{schema_name}"."{table_name}" ({columns});
                """.format(**{
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'columns': columns_partial_query,
                }),
                commit=False,
            )

        if not drop_and_replace:
            # make sure there is a unique constraint for update_on_columns
            self.execute(
                sql="""
                    ALTER TABLE "{schema_name}"."{table_name}"
                    DROP CONSTRAINT IF EXISTS "{constraint}";
                    ALTER TABLE "{schema_name}"."{table_name}"
                    ADD CONSTRAINT "{constraint}" UNIQUE ("{columns}");
                """.format(**{
                    'schema_name': schema_name,
                    'table_name': table_name,
                    'constraint': 'ufu_{0}_{1}'.format(schema_name, table_name),
                    'columns': '", "'.join(update_on_columns),
                }),
                commit=False,
            )

        set_columns = []
        for column in columns_definition.keys():
            if not (column in update_on_columns):
                set_columns += [column]

        sql="""
            INSERT INTO "{schema_name}"."{table_name}"
            ("{column_names}") VALUES %s
            ON CONFLICT {do_on_conflict};
        """.format(**{
            'schema_name': schema_name,
            'table_name': table_name,
            'column_names': '", "'.join(list(columns_definition.keys())),
            'do_on_conflict': 'DO NOTHING' if drop_and_replace else """
                ("{update_columns}") DO UPDATE SET\n\t{sets}
            """.format(
                update_columns='", "'.join(update_on_columns),
                sets='\n\t,'.join([
                    '"{column}" = EXCLUDED."{column}"'.format(column=column)
                    for column in set_columns
                ]),
            ),
        })
        logging_function('Now Uploading!')
        template = '(%('+')s, %('.join(list(columns_definition.keys()))+')s)'
        cur = self.get_cursor()
        while data:
            upload_data = data[:upload_chunking]
            data = data[upload_chunking:]
            execute_values(
                cur=cur,
                sql=sql,
                argslist=upload_data,
                template=template,
            )
        logging_function('Upload done.')

        self.execute(
            sql='ANALYZE "{0}"."{1}";'.format(schema_name, table_name),
            commit=False,
        )


    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def test_if_table_exists(self, table_name, schema_name):
        return bool(self.execute_and_return_result(
                sql="SELECT to_regclass(%(name)s);",
                params={'name':'"{0}"."{1}"'.format(schema_name, table_name)},
            )[0][0])
