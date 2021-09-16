from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC
from ewah.hooks.salesforce import EWAHSalesforceHook

from typing import Optional


class EWAHSalesforceOperator(EWAHBaseOperator):

    _NAMES = ["sf", "salesforce"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHSalesforceHook.conn_type

    _SUBSEQUENT_PLACEHOLDER = "~*subsequent-field-placeholder*~"

    def __init__(
        self, salesforce_object: Optional[str] = None, *args, **kwargs
    ) -> None:
        self.salesforce_object = salesforce_object or kwargs.get("target_table_name")
        kwargs["primary_key_column_name"] = "Id"
        # default subsequent_field to placeholder to be filled during execution
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = kwargs.get(
                "subsequent_field", self._SUBSEQUENT_PLACEHOLDER
            )
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context: dict) -> None:
        self.log.info(f"Fetching Salesforce data for {self.salesforce_object}...")
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.subsequent_field == self._SUBSEQUENT_PLACEHOLDER
        ):
            self.subsequent_field = self.source_hook.get_incrementer(
                salesforce_object=self.salesforce_object
            )

        for batch in self.source_hook.get_data_in_batches(
            salesforce_object=self.salesforce_object,
            columns=list(self.columns_definition or []) or None,
            data_from=self.data_from,
            data_until=self.data_until,
        ):
            self.upload_data(batch)
