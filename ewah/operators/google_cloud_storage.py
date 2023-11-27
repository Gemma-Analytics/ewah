from ewah.constants import EWAHConstants as EC
from ewah.hooks.google_cloud_storage import EWAHGoogleCloudStorageHook
from ewah.operators.base import EWAHBaseOperator


class EWAHGoogleCloudStorageOperator(EWAHBaseOperator):
    _NAMES = ["gcs", "google_cloud_storage"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHGoogleCloudStorageHook.conn_type

    def __init__(self, bucket_name, prefix=None, data_after=None, *args, **kwargs):
        self.bucket_name = bucket_name
        self.data_after = data_after
        self.prefix = prefix
        kwargs["subsequent_field"] = "blob_modified_at"
        kwargs["primary_key"] = "id"
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
        else:
            data_from = self.data_after or self.data_from

        for batch in self.source_hook.get_data_in_batches(
            self.bucket_name, data_after=data_from, prefix=self.prefix
        ):
            self.upload_data(batch)
