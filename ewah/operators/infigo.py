from ewah.hooks.infigo import EWAHInfigoHook

from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC


class EWAHInfigoOperator(EWAHBaseOperator):

    _NAMES = ["infigo"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _CONN_TYPE = EWAHInfigoHook.conn_type

    def __init__(self, resource=None, *args, **kwargs):
        resource = resource or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)
        self.resource = resource

    def ewah_execute(self, context):
        self.upload_data(
            self.source_hook.get_data(
                resource=self.resource,
            )
        )
