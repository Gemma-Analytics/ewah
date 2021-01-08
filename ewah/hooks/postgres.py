from ewah.constants import EWAHConstants as EC
from ewah.hooks.base import EWAHBaseHook

from psycopg2 import connect as pg_connect
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Union, Dict, Any


class EWAHPostgresHook(EWAHBaseHook):

    _ATTR_RELABEL: dict = {
        "database": "schema",
        "hostname": "host",
        "user": "login",
    }

    conn_name_attr = "ewah_postgres_conn_id"
    default_conn_name = "ewah_postgres_default"
    conn_type = "ewah_postgres"
    hook_name = "EWAH PostgreSQL Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "password": "Password",
                "login": "User",
                "schema": "Database",
                "host": "Hostname / IP",
                "post": "Port (default: 5432)",
            },
        }

    @property
    def pgconn(self):
        if not hasattr(self, "_pgconn"):
            self._pgconn = pg_connect(
                "dbname='{0}' user='{1}' host='{2}' password='{3}' port='{4}'".format(
                    self.conn.database,
                    self.conn.user,
                    self.conn.host,
                    self.conn.password,
                    self.conn.port or "5432",
                )
            )
        return self._pgconn

    @property
    def cursor(self):
        """Cursor that returns lists of lists from the data source."""
        if not hasattr(self, "_cur"):
            self._cur = self.pgconn.cursor()
        return self._cur

    @property
    def dictcursor(self):
        """Cursor that returns lists of dictionaries from the data source."""
        if not hasattr(self, "_dictcur"):
            self._dictcur = self.pgconn.cursor(cursor_factory=RealDictCursor)
        return self._dictcur

    def get_records(self, sql, parameters=None):
        """
        Variant of execute method. Required to work with the SQL sensor.
        """
        return self.execute_and_return_result(
            sql=sql, params=parameters, return_dict=False
        )

    def execute(
        self, sql: str, params: Optional[dict] = None, commit: bool = False, cursor=None
    ) -> None:
        self.log.info(f"Executing SQL:\n\n{sql}\n\n")
        (cursor or self.cursor).execute(sql.strip(), vars=params)
        if commit:
            self.commit()

    def execute_and_return_result(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        return_dict: bool = False,
    ) -> Union[List[list], List[dict]]:
        cursor = self.dictcursor if return_dict else self.cursor
        self.execute(sql=sql, params=params, commit=False, cursor=cursor)
        return cursor.fetchall()

    def commit(self):
        self.log.info("Committing changes!")
        return self.pgconn.commit()

    def rollback(self):
        self.log.info("Rolling back changes!")
        return self.pgconn.rollback()

    def get_data_from_sql(
        self, sql: str, params: Optional[dict] = None, return_dict: bool = True
    ) -> Union[List[list], List[dict]]:
        cur = self.dictcursor if return_dict else self.cursor
        cur.execute(sql, vars=params)
        return cur.fetchall()

    def close(self):
        if hasattr(self, "_cur"):
            self._cur.close()
            del self._cur
        if hasattr(self, "_dictcur"):
            self._dictcur.close()
            del self._dictcur
        if hasattr(self, "_pgconn"):
            self.pgconn.close()
            del self._pgconn

    def __del__(self):
        self.close()
