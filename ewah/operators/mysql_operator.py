from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from mysql.connector import connect

class EWAHMySQLOperator(EWAHSQLBaseOperator):

    _SQL_BASE_COLUMNS = 'SELECT `{columns}` FROM `{schema}`.`{table}`\nWHERE {{0}};'
    _SQL_BASE_ALL = 'SELECT * FROM `{schema}`.`{table}`\nWHERE {{0}};'
    _SQL_COLUMN_QUOTE = '`'
    _SQL_MINMAX_CHUNKS = 'SELECT MIN(`{column}`), MAX(`{column}`) FROM `{schema}`.`{table}`;'
    _SQL_CHUNKING_CLAUSE = 'AND `{column}` >= %(from)s AND `{column}` <{equal_sign} %(until)s'

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