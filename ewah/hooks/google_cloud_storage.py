from ewah.constants import EWAHConstants as EC
from ewah.hooks.base import EWAHBaseHook

from google.cloud import storage
from google.oauth2 import service_account

import avro.schema
import json
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from io import BytesIO
from datetime import datetime


class EWAHGoogleCloudStorageHook(EWAHBaseHook):
    _ATTR_RELABEL = {
        "project_id": "schema",
    }

    conn_name_attr = "ewah_google_cloud_storage_conn_id"
    default_conn_name = "ewah_google_cloud_storage_default"
    conn_type = "ewah_google_cloud_storage"
    hook_name = "EWAH Google Cloud Storage Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "password", "login", "host", "port"],
            "relabeling": {"schema": "Project ID"},
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        # from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from ewah.utils.widgets import EWAHTextAreaWidget
        from wtforms import StringField

        return {
            "extra__ewah_google_cloud_storage__service_account_json": StringField(
                "Service Account JSON",
                widget=EWAHTextAreaWidget(rows=12),
            ),
        }

    @property
    def client(self):
        if not hasattr(self, "_client"):
            self._client = storage.Client(
                project=self.conn.project_id,
                credentials=service_account.Credentials.from_service_account_info(
                    json.loads(self.conn.service_account_json)
                ),
            )
        return self._client

    def generate_blob_list(
        self, bucket_name, prefix=None, data_after=None, data_until=None
    ):
        # generate a list of blobs that apply, given the filter
        for blob in self.client.list_blobs(bucket_or_name=bucket_name, prefix=prefix):
            if data_after and blob.updated <= data_after:
                continue
            if data_until and blob.updated > data_until:
                continue
            yield blob

    def get_data_in_batches(
        self,
        bucket_name,
        prefix=None,
        data_after=None,
        data_until=None,
        batch_size=10000,
    ):
        rows = []
        for blob in self.generate_blob_list(
            bucket_name, prefix, data_after, data_until
        ):
            # download file content as bytes, read via avro
            blob_meta = {
                "blob_name": blob.name,
                "blob_modified_at": blob.updated,
            }
            bytes_data = blob.download_as_string()
            bytes_object = BytesIO(bytes_data)
            bytes_object.mode = "rb+"  # need to "fake" the mode attribute because
            # avro checks the mode of the file given for some reason, fails otherwise
            reader = DataFileReader(bytes_object, DatumReader())
            for row in reader:
                # add blob-level metadata
                row.update(blob_meta)
                rows.append(row)
            if len(rows) >= batch_size:
                yield rows
                rows = []

        if rows:
            # return any data that was left after the last iteration
            yield rows
