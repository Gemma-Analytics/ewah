from ewah.operators.sql_base import EWAHSQLBaseOperator
from ewah.hooks.bigquery import EWAHBigQueryHook


class EWAHBigQueryOperator(EWAHSQLBaseOperator):
    _NAMES = ["bq", "biqguery"]

    _SQL_BASE = """
        SELECT
            {columns}
        FROM `{database}`.`{schema}`.`{table}`
    """
    _SQL_BASE_SELECT = "SELECT * FROM ({select_sql}) t \nWHERE {{0}}"
    _SQL_COLUMN_QUOTE = "`"
    _SQL_PARAMS = "@{0}"

    _CONN_TYPE = EWAHBigQueryHook.conn_type

    def __init__(self, *args, **kwargs):
        _msg = "Must supply source_database_name (=project_id )!"
        if kwargs.get("project_id"):
            kwargs["source_database_name"] = kwargs.pop("project_id")
        assert kwargs.get("source_database_name"), _msg
        super().__init__(*args, **kwargs)
