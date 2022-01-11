from ewah.hooks.aircall import EWAHAircallHook

from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC


class EWAHAircallOperator(EWAHBaseOperator):

    _NAMES = ["aircall"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: False,  # Full Refreshs would likely hit API limits and fail!
        # If data is small enough for a full refresh, do it every time.
    }

    _CONN_TYPE = EWAHAircallHook.conn_type

    def __init__(self, resource=None, wait_between_pages=1, *args, **kwargs):
        kwargs["primary_key"] = "id"
        resource = resource or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)

        assert resource in EWAHAircallHook._RESOURCES, "Invalid resource!"
        self.resource = resource
        assert isinstance(wait_between_pages, int) and wait_between_pages >= 0
        self.wait_between_pages = wait_between_pages
        if self.extract_strategy == EC.ES_INCREMENTAL:
            _msg = '"{0}" cannot be loaded incrementally!'.format(resource)
            assert EWAHAircallHook._RESOURCES[resource].get("incremental"), _msg

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            data_from=self.data_from,
            data_until=self.data_until,
            batch_call_pause_seconds=self.wait_between_pages,
        ):
            self.upload_data(batch)
