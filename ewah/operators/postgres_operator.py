from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
# Use EWAHDWHookPostgres as hook since it's already there - not necessarily SOP!
from ewah.dwhooks.dwhook_postgres import EWAHDWHookPostgres

class EWAHPostgresOperator(EWAHSQLBaseOperator):

    _SQL_BASE = \
        'SELECT\n{columns}\nFROM "{schema}"."{table}"\nWHERE {where_clause}'
    _SQL_BASE_SELECT = \
        'WITH raw AS ({select_sql}) SELECT * FROM raw WHERE {{0}}'
    _SQL_COLUMN_QUOTE = '"'
    _SQL_MINMAX_CHUNKS = '''
        WITH base AS ({base})
        SELECT MIN({column}), MAX({column})
        FROM base;
    '''
    _SQL_CHUNKING_CLAUSE = '''
        AND {column} >= %(from_value)s
        AND {column} <{equal_sign} %(until_value)s
    '''
    _SQL_PARAMS = '%({0})s'

    def __init__(self, *args, **kwargs):
        self.sql_engine = self._PGSQL
        super().__init__(*args, **kwargs)

    def _get_data_from_sql(self, sql, params=None, return_dict=True):
        hook = EWAHDWHookPostgres(self.source_conn_id)
        self.log.info('Executing:\n{0}\n\nWith params:\n{1}'.format(
            sql,
            str(params),
        ))
        data = hook.execute_and_return_result(
            sql=sql,
            params=params,
            return_dict=return_dict,
        )
        hook.close()
        return data
