from ewah.hooks.sql_base import EWAHSQLBaseHook
from ewah.constants import EWAHConstants as EC

from google.cloud import bigquery
from google.oauth2 import service_account
from tempfile import NamedTemporaryFile
from datetime import timedelta

from typing import List, Dict, Union, Type, Optional

import os
import json


class EWAHBigQueryHook(EWAHSQLBaseHook):

    _ATTR_RELABEL: dict = {"project": "host"}

    conn_name_attr = "ewah_bigquery_conn_id"
    default_conn_name = "ewah_bigquery_default"
    conn_type = "ewah_bigquery"
    hook_name = "EWAH BigQuery Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "password", "login", "schema", "port"],
            "relabeling": {"host": "Project ID"},
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        # from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from ewah.utils.widgets import EWAHTextAreaWidget
        from wtforms import StringField

        return {
            "extra__ewah_bigquery__service_account_json": StringField(
                "Service Account JSON",
                widget=EWAHTextAreaWidget(rows=12),
            ),
            "extra__ewah_bigquery__location": StringField(
                "[optional] location",
                widget=EWAHTextAreaWidget(rows=1),
            ),
        }

    def __init__(self, *args, project_id=None, **kwargs):
        self.project_id = project_id
        return super().__init__(*args, **kwargs)

    class bq_cursor(object):
        """Wrapper around the BigQuery client to work similarly to other db cursor.

        :param client: An initialized and authenticated bigquery Client object.
        :param return_dict: Set to true to return lists of dictionaries in fetach calls,
            otherwise fetches return lists of lists. Defaults to True.
        """

        def __init__(
            self,
            outer_class,
            client: Type[bigquery.client.Client],
            return_dict: bool = True,
        ):
            self.client = client
            self.outer = outer_class  # access outer class
            self.return_dict = return_dict
            self.latest_query = None

        def execute(self, sql, params=None, commit=True):
            if not commit:
                self.outer.log.info("Warning: BigQuery always auto-commits!")

            # params have to be supplied via a job_config object!
            # only scalar implemented - array and structs are TODO!
            if params:
                type_mapping = EC.QBC_TYPE_MAPPING.get(EC.DWH_ENGINE_BIGQUERY)
                _default = type_mapping[str]
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter(
                            key,
                            type_mapping.get(type(value), _default),
                            value,
                        )
                        for key, value in params.items()
                    ]
                )
            else:
                job_config = None

            self.latest_query = self.client.query(sql, job_config=job_config)

        def fetchall(self):
            return self.fetch_all()

        def fetch_all(self):
            if not self.latest_query:
                return []

            dict_result = [dict(row) for row in self.latest_query.result()]
            if self.return_dict:
                return dict_result

            # Note: Must deal with empty results
            keys = (dict_result[0] if dict_result else {}).keys()
            return [[row[key] for key in keys] for row in dict_result]

        def fetch_in_batches(self, batch_size: int):
            """Generator to iterate over pages of a custom size."""
            if not self.latest_query:
                return []
            for page in self.latest_query.result(page_size=batch_size).pages:
                self.outer.log.info("Yielding a page...")
                dict_result = [dict(row) for row in page]
                if self.return_dict:
                    yield dict_result
                else:
                    keys = dict_result[0].keys()
                    yield [[row[key] for key in keys] for row in dict_result]

    def _get_db_conn(self):
        conn_kwargs = {
            "credentials": service_account.Credentials.from_service_account_info(
                json.loads(self.conn.service_account_json)
            )
        }
        if self.conn.location:
            conn_kwargs["location"] = self.conn.location
        if self.project_id or self.conn.project:
            conn_kwargs["project"] = self.project_id or self.conn.project
        return bigquery.client.Client(**conn_kwargs)

    def _get_cursor(self):
        return self.bq_cursor(outer_class=self, client=self.dbconn, return_dict=False)

    def _get_dictcursor(self):
        return self.bq_cursor(outer_class=self, client=self.dbconn, return_dict=True)

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
        (cursor or self.cursor).execute(sql=sql.strip(), params=params, commit=commit)

    def get_data_from_sql(
        self, sql: str, params: Optional[dict] = None, return_dict: bool = True
    ) -> Union[List[list], List[dict]]:
        cur = self.dictcursor if return_dict else self.cursor
        self.execute(sql, params=params, cursor=cur, commit=False)
        return cur.fetchall()

    def get_data_in_batches(
        self,
        sql: str,
        params: Optional[dict] = None,
        return_dict: bool = True,
        batch_size: int = 25000,
    ):
        cur = self.dictcursor if return_dict else self.cursor
        self.execute(sql, params=params, cursor=cur)
        data = []
        for batch in cur.fetch_in_batches(batch_size=batch_size):
            data += batch
            if len(data) >= batch_size:
                yield data
                data = []
        if data:
            yield data
