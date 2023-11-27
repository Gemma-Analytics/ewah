from ewah.hooks.sql_base import EWAHSQLBaseHook

import pymssql
import uuid

from typing import Optional, List, Union


class EWAHMSSQLHook(EWAHSQLBaseHook):
    _DEFAULT_PORT = 1433

    _ATTR_RELABEL = {
        "user": "login",
        "database": "schema",
    }

    conn_name_attr = "ewah_mssql_conn_id"
    default_conn_name = "ewah_mssql_default"
    conn_type = "ewah_mssql"
    hook_name = "EWAH MSSQL Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "password": "Password",
                "login": "User",
                "schema": "Database",
                "host": "Hostname / IP",
                "port": "Port (default: 1433)",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from wtforms import StringField

        return {
            f"extra__ewah_mssql__ssh_conn_id": StringField(
                "SSH Connection ID (optional)",
                widget=BS3TextFieldWidget(),
            ),
        }

    @staticmethod
    def get_cleaner_callables():
        # type UUID needs to be cast into a string!
        def cast_uuid_to_string(row):
            for key, value in row.items():
                if isinstance(value, uuid.UUID):
                    row[key] = str(value)

            return row

        return [cast_uuid_to_string]

    def _get_db_conn(self):
        conn = pymssql.connect(
            f"{self.local_bind_address[0]}:{self.local_bind_address[1]}",
            self.conn.user,
            self.conn.password,
            self.conn.database,
        )
        return conn

    def _get_cursor(self):
        return self.dbconn.cursor()

    def _get_dictcursor(self):
        return self.dbconn.cursor(as_dict=True)

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
        (cursor or self.cursor).execute(sql.strip(), params)
        if commit:
            self.commit()
