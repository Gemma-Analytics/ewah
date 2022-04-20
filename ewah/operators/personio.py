from ewah.hooks.personio import EWAHPersonioHook

from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC


class EWAHPersonioOperator(EWAHBaseOperator):

    _NAMES = ["personio"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHPersonioHook.conn_type

    def __init__(self, resource=None, *args, **kwargs):
        resource = resource or kwargs.get("target_table_name", "").lower()
        assert EWAHPersonioHook.validate_resource(resource)
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            assert (
                resource == "attendances"
            ), "Only attendances work with subsequent loading!"
            kwargs["subsequent_field"] = "updated_at"
            kwargs["primary_key"] = "id"

        super().__init__(*args, **kwargs)

        self.resource = resource

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
        else:
            data_from = None
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            data_from=data_from,
        ):
            self.upload_data(batch)
