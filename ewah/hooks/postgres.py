from ewah.hooks.sql_base import EWAHSQLBaseHook

from psycopg2 import connect as pg_connect
from psycopg2.extras import RealDictCursor
from typing import Optional, List, Union, Dict, Any


class EWAHPostgresHook(EWAHBaseHook):

    _DEFAULT_PORT = 5432

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

    def _get_db_conn(self):
        return pg_connect(
            "dbname='{0}' user='{1}' host='{2}' password='{3}' port='{4}'".format(
                self.conn.database,
                self.conn.user,
                self.local_bind_address[0],
                self.conn.password,
                self.local_bind_address[1],
            )
        )

    def _get_cursor(self):
        return self.dbconn.cursor()

    def _get_dictcursor(self):
        return self.dbconn.cursor(cursor_factory=RealDictCursor)

    def execute(
        self, sql: str, params: Optional[dict] = None, commit: bool = False, cursor=None
    ) -> None:
        self.log.info(f"Executing SQL:\n\n{sql}\n\n")
        (cursor or self.cursor).execute(sql.strip(), vars=params)
        if commit:
            self.commit()

    def get_data_from_sql(
        self, sql: str, params: Optional[dict] = None, return_dict: bool = True
    ) -> Union[List[list], List[dict]]:
        cur = self.dictcursor if return_dict else self.cursor
        cur.execute(sql, vars=params)
        return cur.fetchall()
