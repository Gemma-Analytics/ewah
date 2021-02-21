from ewah.constants import EWAHConstants as EC
from ewah.hooks.airflow import EWAHAirflowHook
from ewah.operators.base import EWAHBaseOperator


class EWAHAirflowOperator(EWAHBaseOperator):

    _NAMES = ["airflow"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _CONN_TYPE = EWAHAirflowHook.conn_type

    def __init__(self, endpoint=None, request_page_size=100, *args, **kwargs):
        self.endpoint = endpoint or kwargs.get("target_table_name")
        self.request_page_size = request_page_size
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(
            endpoint=self.endpoint,
            page_size=self.request_page_size,
        ):
            self.upload_data(batch)
