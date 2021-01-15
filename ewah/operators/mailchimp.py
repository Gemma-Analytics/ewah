from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.mailchimp import EWAHMailchimpHook


class EWAHMailchimpOperator(EWAHBaseOperator):

    _NAMES = ["mailchimp", "mc"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _REQUIRES_COLUMNS_DEFINITION = False

    _CONN_TYPE = EWAHMailchimpHook.conn_type

    def __init__(self, resource=None, *args, **kwargs):
        # use target table name as resource if none is given (-> easier config)
        self.resource = resource or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(resource=self.resource):
            self.upload_data(batch)
