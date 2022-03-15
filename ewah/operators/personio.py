from ewah.hooks.personio import EWAHPersonioHook

from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC


class EWAHRecurlyOperator(EWAHBaseOperator):

    _NAMES = ["personio"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: False,
    }

    _CONN_TYPE = EWAHPersonioHook.conn_type

    def __init__(self, resource=None, *args, **kwargs):
        # kwargs["primary_key"] = "id"
        resource = resource or kwargs.get("target_table_name", "").lower()
        super().__init__(*args, **kwargs)

        assert EWAHPersonioHook.validate_resource(resource)
        self.resource = resource

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
        ):
            self.upload_data(batch)
