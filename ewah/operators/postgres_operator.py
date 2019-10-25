from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
# Use EWAHDWHookPostgres as hook since it's already there - not necessarily SOP!
from ewah.dwhooks.dwhook_postgres import EWAHDWHookPostgres

class EWAHPostgresOperator(EWAHSQLBaseOperator):

    _SQL_BASE_COLUMNS = 'SELECT "{0}" FROM "{1}"."{2}"\nWHERE {{0}};'
    _SQL_BASE_ALL = 'SELECT * FROM "{0}"."{1}"\nWHERE {{0}};'
    _SQL_COLUMN_QUOTE = '"'
    _SQL_MINMAX_CHUNKS = 'SELECT MIN("{0}"), MAX("{0}") FROM "{1}"."{2}";'
    _SQL_CHUNKING_CLAUSE = 'AND "{0}" >= %(from)s AND "{0}" <{1} %(until)s'

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
