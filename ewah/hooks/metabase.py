from ewah.hooks.base import EWAHBaseHook

from dbtmetabase.models.interface import MetabaseInterface, DbtInterface

import os
import yaml


class EWAHMetabaseHook(EWAHBaseHook):
    _ATTR_RELABEL = {
        "user": "login",
        "database": "schema",
        "sync_timeout": "port",
    }

    conn_name_attr = "ewah_metabase_conn_id"
    default_conn_name = "ewah_metabase_default"
    conn_type = "ewah_metabase"
    hook_name = "EWAH Metabase Connection"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["extra"],
            "relabeling": {
                "login": "User",
                "password": "Password",
                "host": "Host / URL",
                "schema": "Database Name (in Metabase)",
                "port": "Sync timeout (leave empty for none)",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from wtforms import StringField
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget

        return {
            "extra__ewah_metabase__http_string": StringField(
                "Use http instead of https?",
                widget=BS3TextFieldWidget(),
            )
        }

    def push_dbt_docs_to_metabase(
        self,
        dbt_project_path: str,
        dbt_database_name: str,
    ):
        models, aliases = DbtInterface(
            path=None,
            manifest_path=os.sep.join(
                (
                    dbt_project_path,
                    yaml.load(
                        open(os.sep.join((dbt_project_path, "dbt_project.yml")), "rb"),
                        Loader=yaml.Loader,
                    )["target-path"],
                    "manifest.json",
                )
            ),
            database=dbt_database_name,
            schema_excludes=None,
            includes=None,
            excludes=None,
        ).read_models(
            include_tags=True,
            docs_url=None,
        )

        metabase = MetabaseInterface(
            host=self.conn.host.replace("http://", "").replace("https://", ""),
            user=self.conn.user,
            password=self.conn.password,
            use_http=(self.conn.http_string or "").lower().startswith(("y", "j")),
            verify=True,
            database=self.conn.database,
            sync=True,
            sync_timeout=self.conn.sync_timeout or None,
        )

        metabase.prepare_metabase_client(models)

        self.log.info("Pushing docs to dbt now!")
        metabase.client.export_models(
            database=metabase.database,
            models=models,
            aliases=aliases,
        )
