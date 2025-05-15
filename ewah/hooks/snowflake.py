from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from typing import Optional

import snowflake.connector

from ewah.hooks.base import EWAHBaseHook


class EWAHSnowflakeHook(EWAHBaseHook):
    _ATTR_RELABEL = {
        "user": "login",
        "account": "host",
    }

    conn_name_attr = "ewah_snowflake_conn_id"
    default_conn_name = "ewah_snowflake_default"
    conn_type = "ewah_snowflake"
    hook_name = "EWAH Snowflake Connection"

    def __init__(self, *args, database: Optional[str] = None, **kwargs):
        self.database = database
        return super().__init__(*args, **kwargs)

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "port", "schema"],
            "relabeling": {
                "password": "Password",
                "login": "User",
                "host": "Account",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import (
            BS3TextFieldWidget,
            BS3PasswordFieldWidget,
        )
        from wtforms import StringField, PasswordField
        from ewah.utils.widgets import EWAHTextAreaWidget

        return {
            "extra__ewah_snowflake__database": StringField(
                "Default Database",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_snowflake__role": StringField(
                "Role",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_snowflake__warehouse": StringField(
                "Warehouse",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_snowflake__private_key": StringField(
                "Private Key (Text)",
                widget=EWAHTextAreaWidget(rows=12),
            ),
            "extra__ewah_snowflake__private_key_passphrase": PasswordField(
                "Private Key Passphrase",
                widget=BS3PasswordFieldWidget(),
            ),
        }

    @property
    def snow_conn(self):
        connection_params = {
            "account": self.conn.account,
            "database": self.database or self.conn.database,
            "warehouse": self.conn.warehouse,
            "user": self.conn.user,
            "role": self.conn.role,
        }

        if self.conn.private_key:
            # Adapted from: https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/_modules/airflow/providers/snowflake/hooks/snowflake.html
            passphrase = None
            if self.conn.private_key_passphrase:
                passphrase = self.conn.private_key_passphrase.strip().encode()
            
            private_key_pem = self.conn.private_key.strip().encode()
            
            p_key = serialization.load_pem_private_key(
                private_key_pem, password=passphrase, backend=default_backend()
            )
            
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            
            connection_params = {
                **connection_params,
                "private_key": pkb,
            }
        else:
            connection_params = {
                **connection_params,
                "password": self.conn.password,
            }

        if not hasattr(self, "_snow_conn"):
            self._snow_conn = snowflake.connector.connect(**connection_params)

        return self._snow_conn

    @staticmethod
    def _adjust_sql(sql):
        sql = sql.strip()
        if sql[-1:] == ";":
            sql = sql[:-1].strip()
        return sql

    @property
    def cursor(self):
        if not hasattr(self, "_cur"):
            self._cur = self.snow_conn.cursor()
            # Start a transaction!
            self._cur.execute("BEGIN;")
        return self._cur

    def commit(self):
        self.cursor.execute("COMMIT;")
        self.cursor.execute("BEGIN;")

    def rollback(self):
        self.cursor.execute("ROLLBACK;")
        self.cursor.execute("BEGIN;")

    def execute(
        self, sql: str, params: Optional[dict] = None, commit: bool = False, cursor=None
    ) -> None:
        self.log.info(
            "Executing SQL:\n\n{0}\n\nWith params:\n{1}".format(
                sql,
                (
                    "\n".join(
                        [
                            "{0}: {1}".format(key, str(value))
                            for (key, value) in params.items()
                        ]
                    )
                    if params
                    else "No params!"
                ),
            )
        )
        sql = self._adjust_sql(sql)
        for statement in sql.split(";"):
            (cursor or self.cursor).execute(statement.strip(), *[params])
        if commit:
            self.commit()

    def execute_and_return_result(self, sql, params=None, return_dict=False):
        if ";" in sql.strip()[:-1]:
            # Need to refactor this to properly deal with the single-statement constraint
            raise Exception("Invalid character ';' in statement!")
        if return_dict:
            raise Exception("return_dict = True not yet implemented for Snowflake!")
        cursor = self.cursor
        self.execute(sql=sql, params=params, commit=False, cursor=cursor)
        return [row for row in cursor]

    def close(self):
        if hasattr(self, "_cur"):
            try:
                # Uncommitted changes must be rolled back before closing cursor!
                self._cur.execute("ROLLBACK;")
            finally:
                self._cur.close()
                del self._cur
        if hasattr(self, "_snow_conn"):
            self._snow_conn.close()
            del self._snow_conn

    def __del__(self):
        self.close()
