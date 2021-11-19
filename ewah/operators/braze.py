from ewah.hooks.braze import EWAHBrazeHook
from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from typing import Optional


class EWAHBrazeOperator(EWAHBaseOperator):

    _NAMES = ["braze"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _CONN_TYPE = EWAHBrazeHook.conn_type

    def __init__(self, object: Optional[str] = None, *args, **kwargs):
        kwargs["primary_key_column_name"] = "id"
        object = object or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)

        assert EWAHBrazeHook.validate_resource(
            object
        ), "{0} is an invalid object!".format(object)
        self.object = object

    def ewah_execute(self, context):
        for batch in self.source_hook.get_object_data(self.object):
            self.upload_data(batch)
