from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.linkedin import EWAHLinkedInHook

from datetime import datetime, timedelta, date


class EWAHLinkedInOperator(EWAHBaseOperator):
    _NAMES = ["li", "linkedin"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: False,
    }

    _CONN_TYPE = EWAHLinkedInHook.conn_type

    def __init__(self, object_name=None, *args, **kwargs):
        object_name = object_name or kwargs["target_table_name"]
        assert (
            object_name in EWAHLinkedInHook._AVAILABLE_OBJECTS
        ), "Invalid object {0}!".format(object_name)
        super().__init__(*args, **kwargs)

        self.object_name = object_name

    def ewah_execute(self, context):
        self.upload_data(self.source_hook.get_object_data(self.object_name))
