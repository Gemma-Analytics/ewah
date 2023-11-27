from ewah.operators.sql_base import EWAHSQLBaseOperator
from ewah.hooks.mssql import EWAHMSSQLHook


class EWAHMSSQLOperator(EWAHSQLBaseOperator):
    _NAMES = ["mssql"]

    _SQL_BASE = 'SELECT\n{columns}\nFROM "{schema}"."{table}"'
    _SQL_BASE_SELECT = "SELECT * FROM (\n\n{select_sql}\n\n) t\n\nWHERE {{0}}"
    _SQL_COLUMN_QUOTE = '"'
    _SQL_PARAMS = "%({0})s"

    _CONN_TYPE = EWAHMSSQLHook.conn_type
