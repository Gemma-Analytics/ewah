from ewah.hooks.braze import EWAHBrazeHook
from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from typing import Optional


class EWAHBrazeOperator(EWAHBaseOperator):
    _NAMES = ["braze"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHBrazeHook.conn_type

    def __init__(self, object: Optional[str] = None, *args, **kwargs):
        kwargs["primary_key"] = "id"
        object = object or kwargs.get("target_table_name")
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT and not kwargs.get(
            "subsequent_field"
        ):
            kwargs["subsequent_field"] = "updated_at"
        super().__init__(*args, **kwargs)

        assert EWAHBrazeHook.validate_resource(
            object
        ), "{0} is an invalid object!".format(object)
        self.object = object

    def ewah_execute(self, context):
        if self.subsequent_field and self.test_if_target_table_exists():
            data_from = self.get_max_value_of_column(self.subsequent_field)
        else:
            data_from = None
        for batch in self.source_hook.get_object_data(self.object, data_from):
            self.upload_data(batch)
