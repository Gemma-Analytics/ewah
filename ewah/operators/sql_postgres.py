from ewah.operators.sql_base import EWAHSQLBaseOperator
from ewah.hooks.postgres import EWAHPostgresHook


class EWAHPostgresOperator(EWAHSQLBaseOperator):

    _NAMES = ["pgsql", "postgres", "postgresql"]

    _SQL_BASE = 'SELECT\n{columns}\nFROM "{schema}"."{table}"\n'
    _SQL_BASE_SELECT = (
        "WITH raw_data AS (\n\n{select_sql}\n\n) SELECT * FROM raw_data\nWHERE {{0}}"
    )
    _SQL_COLUMN_QUOTE = '"'
    _SQL_PARAMS = "%({0})s"

    _CONN_TYPE = EWAHPostgresHook.conn_type
