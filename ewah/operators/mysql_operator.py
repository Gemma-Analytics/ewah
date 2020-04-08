from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from mysql.connector import connect

class EWAHMySQLOperator(EWAHSQLBaseOperator):

    _SQL_BASE_COLUMNS = 'SELECT\n`{columns}`\nFROM `{schema}`.`{table}`\nWHERE {{0}};'
    _SQL_BASE_ALL = 'SELECT * FROM `{schema}`.`{table}`\nWHERE {{0}};'
    _SQL_COLUMN_QUOTE = '`'
    _SQL_MINMAX_CHUNKS = 'SELECT MIN({column}), MAX({column}) FROM `{schema}`.`{table}` WHERE {where_clause};'
    _SQL_CHUNKING_CLAUSE = 'AND {column} >= %(from_value)s AND {column} <{equal_sign} %(until_value)s'
    _SQL_PARAMS = '%({0})s'

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
        self.log.info('Executing:\n{0}\n\nWith params:\n{1}'.format(
            sql,
            str(params),
        ))
        cursor.execute(sql, params=params)
        data = cursor.fetchall()
        cursor.close()
        database_conn.close()
        return data

    def execute(self, context):
        self.source_schema_name = self.source_schema_name or \
            BaseHook.get_connection(self.source_conn_id).schema
        super().execute(context=context)
