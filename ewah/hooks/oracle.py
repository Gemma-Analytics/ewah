from ewah.hooks.sql_base import EWAHSQLBaseHook

import cx_Oracle

from typing import Optional, List, Union


class EWAHOracleSQLOperator(EWAHSQLBaseHook):

    _DEFAULT_PORT = 1521

    _ATTR_RELABEL = {
        "user": "login",
        "sid": "schema",
    }

    conn_name_attr = "ewah_oracle_conn_id"
    default_conn_name = "ewah_oracle_default"
    conn_type = "ewah_oracle"
    hook_name = "EWAH Oracle Connection"

    _LIMIT_SQL = """
        SELECT * FROM ({sql_query}) t
        ORDER BY {order_by_columns}
        OFFSET {offset} ROWS
        FETCH NEXT {limit} ROWS ONLY
    """

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "password": "Password",
                "login": "User",
                "schema": "SID",
                "host": "Hostname / IP",
                "port": "Port (default: 1521)",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from wtforms import StringField

        return {
            f"extra__ewah_oracle__ssh_conn_id": StringField(
                "SSH Connection ID (optional)",
                widget=BS3TextFieldWidget(),
            ),
        }

    @staticmethod
    def _adjust_sql(sql):
        sql = sql.strip()
        if sql[-1:] == ";":
            sql = sql[:-1].strip()
        return sql

    def _get_db_conn(self):
        return cx_Oracle.connect(
            self.conn.user,
            self.conn.password,
            "{0}:{1}/{2}".format(
                self.local_bind_address[0],
                self.local_bind_address[1],
                self.conn.sid,
            ),
            encoding="UTF-8",
        )

    def _get_cursor(self):
        return self.dbconn.cursor()

    def _get_dictcursor(self):
        class dictcur(object):
            # need to monkeypatch the built-in execute function to always return a dict
            def __init__(self, cursor):
                self._original_cursor = cursor

            def execute(self, *args, **kwargs):
                # rowfactory needs to be set AFTER EACH execution!
                self._original_cursor.execute(*args, **kwargs)
                self._original_cursor.rowfactory = lambda *a: dict(
                    zip([d[0] for d in self._original_cursor.description], a)
                )
                # cx_Oracle's cursor's execute method returns a cursor object
                # -> return the correct cursor in the monkeypatched version as well!
                return self._original_cursor

            def __getattr__(self, attr):
                # anything other than the execute method: just go straight to the cursor
                return getattr(self._original_cursor, attr)

        return dictcur(self.dbconn.cursor())

    def execute(
        self, sql: str, params: Optional[dict] = None, commit: bool = False, cursor=None
    ) -> None:
        params = params or {}
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
        (cursor or self.cursor).execute(self._adjust_sql(sql), **params)
        if commit:
            self.commit()

    def get_data_from_sql(
        self, sql: str, params: Optional[dict] = None, return_dict: bool = True
    ) -> Union[List[list], List[dict]]:
        cur = self.dictcursor if return_dict else self.cursor
        self.execute(sql, params=params, cursor=cur, commit=False)
        return cur.fetchall()
