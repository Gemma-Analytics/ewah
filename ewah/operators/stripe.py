from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.stripe import EWAHStripeHook


class EWAHStripeOperator(EWAHBaseOperator):
    _NAMES = ["stripe"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _CONN_TYPE = EWAHStripeHook.conn_type

    def __init__(self, *args, resource=None, expand=None, batch_size=10000, **kwargs):
        if resource is None:
            resource = kwargs.get("target_table_name")
        kwargs["primary_key"] = "id"
        super().__init__(*args, **kwargs)
        self.resource = resource
        self.expand = expand
        self.batch_size = batch_size

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            expand=self.expand,
            batch_size=self.batch_size,
        ):
            self.upload_data(batch)
