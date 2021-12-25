from ewah.constants import EWAHConstants as EC
from ewah.hooks.plentymarkets import EWAHPlentyMarketsHook
from ewah.operators.base import EWAHBaseOperator

from datetime import datetime, date


class EWAHPlentyMarketsOperator(EWAHBaseOperator):

    _NAMES = ["plentymarkets"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHPlentyMarketsHook.conn_type

    def __init__(self, resource=None, additional_api_call_params=None, *args, **kwargs):
        kwargs["primary_key"] = kwargs.get("primary_key", "id")
        resource = resource or kwargs.get("target_table_name")
        if kwargs["extract_strategy"] == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = kwargs.get("subsequent_field", "updatedAt")
            # currently, only the orders and accounts/contacts resource works with subsequent loading
            assert any(["orders" in resource, "accounts/contacts" in resource])
        if kwargs["extract_strategy"] == EC.ES_INCREMENTAL:
            # currently, only the orders resource works with incremental loading
            assert (
                EWAHPlentyMarketsHook.format_resource(resource)
                in EWAHPlentyMarketsHook._INCREMENTAL_FIELDS.keys()
            ), "{0} is not incrementally loadable!".format(resource)
        super().__init__(*args, **kwargs)

        assert isinstance(additional_api_call_params, (type(None), dict))
        self.resource = resource
        self.additional_api_call_params = additional_api_call_params

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
            # This is likely to be a text field - need a date instead
            if not isinstance(data_from, date):
                if isinstance(data_from, datetime):
                    data_from = data_from.date()
                elif isinstance(data_from, str):
                    data_from = datetime.fromisoformat(data_from).date()
                else:
                    raise Exception(
                        "Data type of {0} is invalid!".format(self.subsequent_field)
                    )
        else:
            data_from = self.data_from
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            data_from=data_from,
            data_until=self.data_until,
            additional_params=self.additional_api_call_params,
        ):
            self.upload_data(batch)
