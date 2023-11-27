from ewah.hooks.recurly import EWAHRecurlyHook

from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC


class EWAHRecurlyOperator(EWAHBaseOperator):
    _NAMES = ["recurly"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
    }

    _CONN_TYPE = EWAHRecurlyHook.conn_type

    def __init__(self, resource=None, *args, **kwargs):
        kwargs["primary_key"] = "id"
        resource = resource or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)

        assert EWAHRecurlyHook.validate_resource(resource)
        self.resource = resource

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            data_from=self.data_from,
            data_until=self.data_until,
        ):
            self.upload_data(batch)
