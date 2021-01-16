from ewah.operators.sql_base import EWAHSQLBaseOperator
from ewah.hooks.oracle import EWAHOracleSQLOperator


class EWAHOracleSQLOperator(EWAHSQLBaseOperator):

    _NAMES = ["oracle"]

    _SQL_BASE = 'SELECT\n{columns}\nFROM "{schema}"."{table}"\n'
    _SQL_BASE_SELECT = "SELECT * FROM (\n\n{select_sql}\n\n) t\nWHERE {{0}}"
    _SQL_COLUMN_QUOTE = '"'
    _SQL_PARAMS = ":{0}"

    _CONN_TYPE = EWAHOracleSQLOperator.conn_type
