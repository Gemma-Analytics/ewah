from ewah.hooks.sevdesk import EWAHSevDeskHook

from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC


class EWAHSevDeskOperator(EWAHBaseOperator):

    _NAMES = ["sevdesk"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: False,
    }

    _CONN_TYPE = EWAHSevDeskHook.conn_type

    def __init__(self, endpoint=None, embed=None, *args, **kwargs):
        endpoint = endpoint or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)

        assert EWAHSevDeskHook.validate_endpoint(
            endpoint
        ), "Invalid endpoint {0}!".format(endpoint)

        self.endpoint = endpoint
        self.embed = embed

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(
            endpoint=self.endpoint,
            embed=self.embed,
        ):
            self.upload_data(batch)
