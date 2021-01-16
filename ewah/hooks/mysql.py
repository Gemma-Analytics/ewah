from ewah.hook.sql_base import EWAHSQLBaseHook

import pymysql

from typing import Optional, List, Union


class EWAHMySQLHook(EWAHBaseHook):

    _DEFAULT_PORT = 3306

    _ATTR_RELABEL = {
        "user": "login",
        "database": "schema",
    }

    conn_name_attr = "ewah_mysql_conn_id"
    default_conn_name = "ewah_mysql_default"
    conn_type = "ewah_mysql"
    hook_name = "EWAH MySQL Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "password": "Password",
                "login": "User",
                "schema": "Database",
                "host": "Hostname / IP",
                "post": "Port (default: 3306)",
            },
        }

    def _get_db_conn(self):
        conn = pymysql.connect(
            host=self.local_bind_address[0],
            port=self.local_bind_address[1],
            user=self.conn.user,
            passwd=self.conn.password,
            database=self.conn.database,
        )
        conn.begin()  # Begin transaction
        return conn

    def _get_cursor(self):
        return self.dbconn.cursor(cursor=pymysql.cursors.SSCursor)

    def _get_dictcursor(self):
        return self.dbconn.cursor(cursor=pymysql.cursors.SSDictCursor)

    def execute(
        self, sql: str, params: Optional[dict] = None, commit: bool = False, cursor=None
    ) -> None:
        self.log.info(f"Executing SQL:\n\n{sql}\n\n")
        (cursor or self.cursor).execute(sql.strip(), args=params)
        if commit:
            self.commit()
