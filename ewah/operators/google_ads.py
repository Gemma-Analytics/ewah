from ewah.operators.base import EWAHBaseOperator
from ewah.utils.python_utils import is_iterable_not_string
from ewah.constants import EWAHConstants as EC

from ewah.hooks.google_ads import EWAHGoogleAdsHook

from datetime import datetime, timedelta


class EWAHGoogleAdsOperator(EWAHBaseOperator):

    _NAMES = ["gads", "google_ads"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHGoogleAdsHook.conn_type

    def __init__(
        self,
        client_id,
        resource,  # name of the resource, e.g. ad_group
        fields,  # dict of fields, e.g. {'campaign': ['id', 'name']}
        metrics=None,  # list of metrics to retrieve - deprecated!
        conditions=None,  # list of conditions, e.g. ["ad_group.status = 'ENABLED'"]
        *args,
        **kwargs
    ):

        if metrics:
            fields["metrics"] = metrics

        kwargs["primary_key"] = list(
            set(
                [
                    "segments__" + col.replace(".", "__")
                    for col in fields.get("segments", [])
                ]
                + ["{0}__resource_name".format(resource)]
            )
        )

        if conditions and not is_iterable_not_string(conditions):
            raise Exception('Argument "conditions" must be a list!')

        self.fields = fields
        self.resource = resource
        self.client_id = str(client_id)
        self.conditions = conditions

        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = "segments__date"

        super().__init__(*args, **kwargs)

        if not self.extract_strategy == EC.ES_FULL_REFRESH:
            assert "date" in fields.get(
                "segments", []
            ), "If using non-full-refresh loading, must include segments.date!"
            if self.extract_strategy == EC.ES_SUBSEQUENT:
                assert (
                    self.load_data_from or self.reload_data_from
                ), "If using subsequent loading, must supply either 'load_data_from' or 'reload_data_from'!"

    def get_select_statement(self, dict_format, prefix=None):
        # create the list of fields for the SELECT statement
        if prefix is None:
            prefix = ""
        elif not prefix[-1] == ".":
            prefix += "."
        fields = []
        for key, value in dict_format.items():
            for item in value:
                if type(item) == dict:
                    fields += self.get_select_statement(item, prefix + key)
                else:
                    fields += [prefix + key + "." + item]
        return fields

    def ewah_execute(self, context):
        # Make sure the correct date conditions are set
        conditions = self.conditions or []
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field) - (
                self.load_data_from_relative or timedelta(days=0)
            )
            data_until = (self.data_until or datetime.now()).date()
        else:
            data_from = self.data_from.date()
            data_until = (self.data_until or datetime.now()).date()
        if data_from and data_until:
            conditions.append(
                "segments.date BETWEEN '{0}' AND '{1}'".format(
                    data_from.isoformat(),
                    data_until.isoformat(),
                )
            )
        self.upload_data(
            self.source_hook.get_data(
                client_id=self.client_id,
                fields=self.fields,
                resource=self.resource,
                conditions=conditions,
            )
        )
