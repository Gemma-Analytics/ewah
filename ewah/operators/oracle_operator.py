from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import cx_Oracle

class EWAHOracleSQLOperator(EWAHSQLBaseOperator):

        _SQL_BASE_COLUMNS = \
            'SELECT\n"{columns}"\nFROM "{table}"\nWHERE {where_clause};'
        _SQL_BASE_ALL = 'SELECT * FROM "{table}"\nWHERE {where_clause};'
        _SQL_BASE_STATEMENT = \
            'WITH raw AS ({select_sql}) SELECT * FROM raw WHERE {{0}};'
        _SQL_COLUMN_QUOTE = '"'
        _SQL_MINMAX_CHUNKS = '''
            WITH base AS ({base})
            SELECT MIN({column}), MAX({column})
            FROM base;
        '''
        _SQL_CHUNKING_CLAUSE = '''
            AND {column} >= :from_value
            AND {column} <{equal_sign} :until_value
        '''
        _SQL_PARAMS = ':{0}'

        def __init__(self, *args, **kwargs):
            self.sql_engine = self._ORACLE
            if kwargs.get('source_schema_name'):
                raise Exception('source_schema_name is an illegal argument ' \
                    + 'for the Oracle operator!')
            super().__init__(*args, **kwargs)

        def _get_data_from_sql(self,
                sql,
                params={},
                return_dict=True,
            ):
            '''In Oracle, params are passed to the execute() function as kwargs
            https://cx-oracle.readthedocs.io/en/latest/user_guide/bind.html
            Params are then referenced like to:
                SELECT * FROM table WHERE field1 = :val1 AND field2 = :val2
            with a params dict structured like so: {'val1': 1, 'val2': 3}
            Also see here: https://stackoverflow.com/questions/35045879/cx-oracle-how-can-i-receive-each-row-as-a-dictionary
            to understand the workaround regarding return_dict
            '''
            def makeDictFactory(cursor):
                columnNames = [d[0] for d in cursor.description]
                def createRow(*args):
                    return dict(zip(columnNames, args))
                return createRow

            connection = BaseHook.get_connection(self.source_conn_id)
            oracle_conn = cx_Oracle.connect(
                connection.login,
                connection.password,
                '{0}:{1}/{2}'.format(
                    connection.host,
                    connection.port,
                    connection.schema,
                ),
                encoding='UTF-8'
            )
            cursor = oracle_conn.cursor()
            if sql.strip()[-1:] == ';': # OracleSQL doesn't like semicolons
                sql = sql.strip()[:-1]
            self.log.info('Executing:\n{0}\n\nWith params:\n{1}'.format(
                sql,
                str(params),
            ))
            cursor.execute(sql, **params)
            if return_dict:
                cursor.rowfactory = makeDictFactory(cursor)
            data = cursor.fetchall()
            cursor.close()
            oracle_conn.close()
            return data
