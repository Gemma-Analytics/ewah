from ewah.hooks.base import EWAHBaseHook

from airflow.utils.file import TemporaryDirectory

import os
from bson.objectid import ObjectId
from copy import deepcopy
from pymongo import MongoClient
from pymongo import ASCENDING as asc
from tempfile import NamedTemporaryFile


class EWAHMongoDBHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "default_database": "schema",
        "uri": "password",
        "username": "login",
    }

    conn_name_attr: str = "ewah_mongodb_conn_id"
    default_conn_name: str = "ewah_mongodb_default"
    conn_type: str = "ewah_mongodb"
    hook_name: str = "EWAH MongoDB Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "host": "Host",
                "schema": "Default database",
                "login": "Username",
                "password": "Uri OR password",
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
            "extra__ewah_mongodb__conn_style": StringField(
                "Connection Style (one of: uri, credentials)",
                default="uri",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_mongodb__tls": StringField(
                "SSL / TLS?",
                default="no",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_mongodb__tls_insecure": StringField(
                "TLS: Allow insecure connections? Aka 'tlsInsecure'",
                default="no",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_mongodb__ssl_cert": StringField(
                "SSL Certificate",
                widget=EWAHTextAreaWidget(rows=12),
            ),
            "extra__ewah_mongodb__ssl_private": StringField(
                "SSL Private Key",
                widget=EWAHTextAreaWidget(rows=12),
            ),
            "extra__ewah_mongodb__ssl_password": StringField(
                "SSL Certificate / Private Key Password",
                widget=BS3PasswordFieldWidget(),
            ),
            "extra__ewah_mongodb__auth_source": StringField(
                "Auth Source", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_mongodb__auth_mechanism": StringField(
                "Auth Mechanism", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_mongodb__ssh_conn_id": StringField(
                "SSH Connection ID", widget=BS3TextFieldWidget()
            ),
        }

    @classmethod
    def get_cleaner_callables(cls):
        # overwrite me for cleaner callables that are always called
        def clean_data(row):
            for key, value in row.items():
                if isinstance(value, ObjectId):
                    row[key] = str(value)
            return row

        return clean_data

    @property
    def mongoclient(self):
        if not hasattr(self, "_mc"):
            if self.conn.ssh_conn_id:
                if self.conn.conn_style == "uri":
                    raise Exception("Cannot have SSH tunnel with uri connection type!")
                if not hasattr(self, "_ssh_hook"):
                    self._ssh_hook = EWAHBaseHook.get_hook_from_conn_id(
                        conn_id=self.conn.ssh_conn_id
                    )
                    self.local_bind_address = self._ssh_hook.start_tunnel(
                        self.conn.host, self.conn.port
                    )
            else:
                self.local_bind_address = (self.conn.host, self.conn.port)

            conn_kwargs = {"tz_aware": True}
            if self.conn.conn_style == "uri":
                conn_kwargs["host"] = self.conn.uri
            else:
                conn_kwargs["host"] = self.local_bind_address[0]
                conn_kwargs["port"] = self.local_bind_address[1]
                if self.conn.username:
                    conn_kwargs["username"] = self.conn.username
                if self.conn.password:
                    conn_kwargs["password"] = self.conn.password

            with TemporaryDirectory() as tmp_dir:
                if self.conn.tls.lower().startswith(("y", "t")):
                    conn_kwargs["tls"] = True
                with NamedTemporaryFile(dir=tmp_dir) as ssl_cert:
                    if self.conn.ssl_cert:
                        ssl_cert.write(self.conn.ssl_cert.encode())
                        if self.conn.ssl_private:
                            # Concatenate into the same file
                            ssl_cert.write(self.conn.ssl_private.encode())
                        ssl_cert.seek(0)
                        conn_kwargs["tlsCertificateKeyFile"] = os.path.abspath(
                            ssl_cert.name
                        )

                    if self.conn.ssl_password:
                        conn_kwargs[
                            "tlsCertificateKeyFilePassword"
                        ] = self.conn.ssl_password
                    if self.conn.tls_insecure.lower().startswith(("y", "t")):
                        conn_kwargs["tlsInsecure"] = True
                    if self.conn.auth_source:
                        conn_kwargs["authSource"] = self.conn.auth_source
                    if self.conn.auth_mechanism:
                        conn_kwargs["authMechanism"] = self.conn.auth_mechanism
                    self._mc = MongoClient(**conn_kwargs)

        return self._mc

    def get_data_in_batches(
        self,
        collection,
        database=None,
        filter_expression=None,
        batch_size: int = 100000,
    ):

        mongo_database = self.mongoclient.get_database(
            name=database or self.conn.default_database
        )
        mongo_collection = mongo_database[collection]
        self.log.info(
            "\n\nFetching data with filter expression:\n{0}\n\n".format(
                filter_expression
            )
        )
        filter_expression = deepcopy(filter_expression) or {}
        cursor = mongo_collection.find(filter_expression)

        data = []
        for document in cursor:
            data.append(document)
            if len(data) >= batch_size:
                yield data
                data = []
        if data:
            yield data

    def close(self):
        if hasattr(self, "_mc"):
            if hasattr(self, "_ssh_hook"):
                self._ssh_hook.stop_tunnel()
                del self._ssh_hook
            self._mc.close()
            del self._mc
