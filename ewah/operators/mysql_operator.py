from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import pymysql

class EWAHMySQLOperator(EWAHSQLBaseOperator):

    _NAMES = ['mysql']

    _SQL_BASE = \
        'SELECT\n{columns}\nFROM `{schema}`.`{table}`\nWHERE {where_clause}'
    _SQL_BASE_SELECT = \
        'SELECT * FROM ({select_sql}) t WHERE {{0}}'
    _SQL_COLUMN_QUOTE = '`'
    _SQL_MINMAX_CHUNKS = 'SELECT MIN({column}), MAX({column}) FROM ({base}) t;'
    _SQL_CHUNKING_CLAUSE = '''
        AND {column} >= %(from_value)s
        AND {column} <{equal_sign} %(until_value)s
    '''
    _SQL_PARAMS = '%({0})s'

    def __init__(self, *args, **kwargs):
        self.sql_engine = self._MYSQL
        super().__init__(*args, **kwargs)

    def _get_data_from_sql(self, sql, params=None, return_dict=True):
        if return_dict:
            cursor_class = pymysql.cursors.DictCursor
        else:
            cursor_class = pymysql.cursors.Cursor
        database_conn = pymysql.connect(**{
            'host': self.source_conn.host,
            'user': self.source_conn.login,
            'passwd': self.source_conn.password,
            'port': self.source_conn.port,
            'database': self.source_conn.schema,
            'cursorclass': cursor_class,
        })
        cursor = database_conn.cursor()
        self.log.info('Executing:\n{0}\n\nWith params:\n{1}'.format(
            sql,
            str(params),
        ))
        cursor.execute(sql, args=params)
        data = cursor.fetchall()
        cursor.close()
        database_conn.close()
        return data
