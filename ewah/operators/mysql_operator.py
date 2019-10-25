from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from mysql.connector import connect

class EWAHMySQLOperator(EWAHSQLBaseOperator):

    _SQL_BASE_COLUMNS = 'SELECT `{0}` FROM `{1}`.`{2}`\nWHERE {{0}};'
    _SQL_BASE_ALL = 'SELECT * FROM `{0}`.`{1}`\nWHERE {{0}};'
    _SQL_COLUMN_QUOTE = '`'
    _SQL_MINMAX_CHUNKS = 'SELECT MIN(`{0}`), MAX(`{0}`) FROM `{1}`.`{2}`;'
    _SQL_CHUNKING_CLAUSE = 'AND `{0}` >= %(from)s AND `{0}` <{1} %(until)s'

    def __init__(self, *args, **kwargs):
        self.sql_engine = self._MYSQL
        super().__init__(*args, **kwargs)

    def _get_data_from_sql(self, sql, params=None, return_dict=True):
        connection = BaseHook.get_connection(self.source_conn_id)
        database_conn = connect(**{
            'host': connection.host,
            'user': connection.login,
            'passwd': connection.password,
            'port': connection.port,
            'database': connection.schema,
        })
        cursor = database_conn.cursor(dictionary=return_dict)
        self.log.info('Executing:\n{0}'.format(sql))
        cursor.execute(sql, params=params)
        return cursor.fetchall()

    def execute(self, context):
        self.source_schema_name = self.source_schema_name or \
            BaseHook.get_connection(self.source_conn_id).schema
        super().execute(context=context)

    def _execute(self, context): # deprecated

        conn = BaseHook.get_connection(self.source_conn_id)
        conn = {
            'host': conn.host,
            'user': conn.login,
            'passwd': conn.password,
            'port': conn.port,
            'database': conn.schema,
        }

        db = connect(**conn)
        cursor = db.cursor(dictionary=True)
        cursor.execute('SELECT * FROM {0}'.format(self.source_table_name))
        data = cursor.fetchall()

        self.upload_data(data)
