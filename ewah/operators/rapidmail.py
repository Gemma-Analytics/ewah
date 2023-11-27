from ewah.constants import EWAHConstants as EC
from ewah.hooks.rapidmail import EWAHRapidmailHook
from ewah.operators.base import EWAHBaseOperator


class EWAHGoogleAdsOperator(EWAHBaseOperator):
    _NAMES = ["rapidmail"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: False,
    }

    _CONN_TYPE = EWAHRapidmailHook.conn_type

    def __init__(self, endpoint=None, *args, **kwargs):
        self.endpoint = endpoint or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        for batch in self.source_hook.get_all_data(endpoint=self.endpoint):
            self.upload_data(batch)
