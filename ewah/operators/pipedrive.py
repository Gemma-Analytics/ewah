from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC
from ewah.hooks.pipedrive import EWAHPipedriveHook

from typing import Optional


class EWAHPipedriveOperator(EWAHBaseOperator):

    _NAMES = ["pd", "pipedrive"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _CONN_TYPE = EWAHPipedriveHook.conn_type

    def __init__(
        self,
        pipedrive_object: Optional[str] = None,
        additional_params: Optional[dict] = None,
        *args,
        **kwargs
    ) -> None:
        self.pipedrive_object = pipedrive_object or kwargs.get("target_table_name")
        self.additional_params = additional_params or {}
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context: dict) -> None:
        self.log.info("Fetching data from Pipedrive for {self.pipedrive_object}...")
        for batch in self.source_hook.get_data_in_batches(
            self.pipedrive_object, **self.additional_params
        ):
            self.upload_data(batch)
