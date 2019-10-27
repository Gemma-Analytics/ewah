from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
# Use EWAHDWHookPostgres as hook since it's already there - not necessarily SOP!
from ewah.dwhooks.dwhook_postgres import EWAHDWHookPostgres

class EWAHPostgresOperator(EWAHSQLBaseOperator):

    _SQL_BASE_COLUMNS = 'SELECT "{columns}" FROM "{schema}"."{table}"\nWHERE {{0}};'
    _SQL_BASE_ALL = 'SELECT * FROM "{schema}"."{table}"\nWHERE {{0}};'
    _SQL_COLUMN_QUOTE = '"'
    _SQL_MINMAX_CHUNKS = 'SELECT MIN("{column}"), MAX("{column}") FROM "{schema}"."{table}";'
    _SQL_CHUNKING_CLAUSE = 'AND "{column}" >= %(from_value)s AND "{column}" <{equal_sign} %(until_value)s'

    def __init__(self, *args, **kwargs):
        self.sql_engine = self._PGSQL
        super().__init__(*args, **kwargs)

    def _get_data_from_sql(self, sql, params=None, return_dict=True):
        hook = EWAHDWHookPostgres(self.source_conn_id)
        data = hook.execute_and_return_result(
            sql=sql,
            params=params,
            return_dict=return_dict,
        )
        hook.close()
        return data
