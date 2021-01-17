from ewah.operators.sql_base import EWAHSQLBaseOperator
from ewah.hooks.mysql import EWAHMySQLHook


class EWAHMySQLOperator(EWAHSQLBaseOperator):

    _NAMES = ["mysql"]

    _SQL_BASE = "SELECT\n{columns}\nFROM `{schema}`.`{table}`"
    _SQL_BASE_SELECT = "SELECT * FROM (\n\n{select_sql}\n\n) t\n\nWHERE {{0}}"
    _SQL_COLUMN_QUOTE = "`"
    _SQL_PARAMS = "%({0})s"

    _CONN_TYPE = EWAHMySQLHook.conn_type
