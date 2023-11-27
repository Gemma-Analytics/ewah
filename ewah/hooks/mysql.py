from ewah.hooks.sql_base import EWAHSQLBaseHook

import pymysql

from typing import Optional, List, Union


class EWAHMySQLHook(EWAHSQLBaseHook):
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
                "port": "Port (default: 3306)",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from wtforms import StringField

        return {
            f"extra__ewah_mysql__ssh_conn_id": StringField(
                "SSH Connection ID (optional)",
                widget=BS3TextFieldWidget(),
            ),
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
        self.log.info(
            "Executing SQL:\n\n{0}\n\nWith params:\n{1}".format(
                sql,
                "\n".join(
                    [
                        "{0}: {1}".format(key, str(value))
                        for (key, value) in params.items()
                    ]
                )
                if params
                else "No params!",
            )
        )
        (cursor or self.cursor).execute(sql.strip(), args=params)
        if commit:
            self.commit()
