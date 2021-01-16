from ewah.operators.sql_base import EWAHSQLBaseOperator
from ewah.hooks.oracle import EWAHOracleSQLOperator


class EWAHOracleSQLOperator(EWAHSQLBaseOperator):

    _NAMES = ["oracle"]

    _SQL_BASE = """
        SELECT\n{columns}\nFROM "{schema}"."{table}"\nWHERE {where_clause}
    """
    _SQL_BASE_SELECT = "SELECT * FROM ({select_sql}) t WHERE {{0}}"
    _SQL_COLUMN_QUOTE = '"'
    _SQL_MINMAX_CHUNKS = """
        WITH base AS ({base})
        SELECT MIN({column}), MAX({column})
        FROM base
    """
    _SQL_CHUNKING_CLAUSE = """
        AND {column} >= :from_value
        AND {column} <{equal_sign} :until_value
    """
    _SQL_PARAMS = ":{0}"

    _CONN_TYPE = EWAHOracleSQLOperator.conn_type

    def _get_data_from_sql(
        self,
        sql,
        params=None,
        return_dict=True,
    ):
        """In Oracle, params are passed to the execute() function as kwargs
        https://cx-oracle.readthedocs.io/en/latest/user_guide/bind.html
        Params are then referenced like so:
            SELECT * FROM table WHERE field1 = :val1 AND field2 = :val2
        with a params dict structured like so: {'val1': 1, 'val2': 3}
        Also see here: https://stackoverflow.com/questions/35045879/cx-oracle-how-can-i-receive-each-row-as-a-dictionary
        to understand the workaround regarding return_dict
        """
        param = params or {}
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
