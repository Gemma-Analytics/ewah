from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.linkedin import EWAHLinkedInHook

from datetime import datetime, timedelta, date


class EWAHLinkedInAdsOperator(EWAHBaseOperator):

    _NAMES = ["li_ads", "linkedin_ads"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHLinkedInHook.conn_type

    def __init__(self, pivot, fields, account_ids=None, *args, **kwargs):
        fields = list(set(fields + list(EWAHLinkedInHook._REQUIRED_FIELDS_ANALYTICS)))
        pivot = pivot.upper()
        EWAHLinkedInHook.validate_fields_list(fields)
        kwargs["primary_key"] = ["date_start", "pivotValue"]
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = "date_start"
        super().__init__(*args, **kwargs)

        if isinstance(account_ids, str):
            account_ids = [account_ids]

        if not account_ids is None:
            assert isinstance(
                account_ids, list
            ), "account_ids must be a list of strings!"

        self.pivot = pivot
        self.fields = fields
        self.account_ids = account_ids

    def ewah_execute(self, context):
        date_end = (self.data_until or datetime.now()).date()
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            date_start = self.get_max_value_of_column(self.subsequent_field) - (
                self.load_data_from_relative or timedelta(days=0)
            )
        else:
            date_start = self.data_from.date()

        if self.load_data_chunking_timedelta:
            batch_from = date_start
            while batch_from <= date_end:
                batch_until = min(
                    date_end, batch_from + self.load_data_chunking_timedelta
                )
                for batch in self.source_hook.get_analytics_in_batches(
                    pivot=self.pivot,
                    date_start=batch_from,
                    date_end=batch_until,
                    fields=self.fields,
                    account_ids=self.account_ids,
                ):
                    self.upload_data(batch)
                batch_from = batch_until + timedelta(days=1)
        else:
            for batch in self.source_hook.get_analytics_in_batches(
                pivot=self.pivot,
                date_start=date_start,
                date_end=date_end,
                fields=self.fields,
                account_ids=self.account_ids,
            ):
                self.upload_data(batch)
