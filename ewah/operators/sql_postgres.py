from ewah.operators.sql_base import EWAHSQLBaseOperator
from ewah.hooks.postgres import EWAHPostgresHook


class EWAHPostgresOperator(EWAHSQLBaseOperator):

    _NAMES = ["pgsql", "postgres", "postgresql"]

    _SQL_BASE = 'SELECT\n{columns}\nFROM "{schema}"."{table}"\nWHERE {where_clause}'
    _SQL_BASE_SELECT = (
        "WITH raw_data AS ({select_sql}) SELECT * FROM raw_data WHERE {{0}}"
    )
    _SQL_COLUMN_QUOTE = '"'
    _SQL_MINMAX_CHUNKS = """
        WITH base AS ({base})
        SELECT MIN({column}), MAX({column})
        FROM base;
    """
    _SQL_CHUNKING_CLAUSE = """
        AND {column} >= %(from_value)s
        AND {column} <{equal_sign} %(until_value)s
    """
    _SQL_PARAMS = "%({0})s"

    _CONN_TYPE = EWAHPostgresHook.conn_type

    def _get_data_from_sql(self, sql, params=None, return_dict=True):
        self.log.info(
            "Executing:\n{0}\n\nWith params:\n{1}".format(
                sql,
                str(params),
            )
        )
        data = self.source_hook.get_data_from_sql(
            sql=sql,
            params=params,
            return_dict=return_dict,
        )
        return data
