from ewah.constants import EWAHConstants as EC
from ewah.hooks.airflow import EWAHAirflowHook
from ewah.operators.base import EWAHBaseOperator

from dateutil.parser import parse


class EWAHAirflowOperator(EWAHBaseOperator):

    _NAMES = ["airflow"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: True,  # Only for DagRuns!
    }

    _CONN_TYPE = EWAHAirflowHook.conn_type

    def __init__(self, endpoint=None, request_page_size=100, *args, **kwargs):
        self.endpoint = endpoint or kwargs.get("target_table_name")
        self.request_page_size = request_page_size
        assert not kwargs.get("subsequent_field"), "Not allowed for this operator!"
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            # Only for DagRuns!
            assert (
                EWAHAirflowHook._ENDPOINTS.get(self.endpoint, self.endpoint)
                == EWAHAirflowHook._ENDPOINT_DAGRUNS
            ), "Only DagRuns are allowed to be subsequent!"
            kwargs["subsequent_field"] = "end_date"
            kwargs["primary_key"] = ["dag_id", "dag_run_id"]
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        if self.subsequent_field and self.test_if_target_table_exists():
            data_from = self.get_max_value_of_column(self.subsequent_field)
            self.log.info("Loading data from {0}".format(data_from))
            if isinstance(data_from, str):
                data_from = parse(data_from)
        else:
            data_from = None
        for batch in self.source_hook.get_data_in_batches(
            endpoint=self.endpoint,
            page_size=self.request_page_size,
            data_from=data_from,
        ):
            self.upload_data(batch)
