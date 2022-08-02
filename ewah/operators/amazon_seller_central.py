from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator
from ewah.hooks.amazon_seller_central import EWAHAmazonSellerCentralHook

from datetime import datetime, timedelta

import pendulum
import time
import pytz


class EWAHAmazonSellerCentralReportsAPIOperator(EWAHBaseOperator):
    """To be used with the Reporting API"""

    _NAMES = ["seller_central_reporting", "amazon_reporting"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        # Depends on individual report, dict is updated during __init__
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: False,
    }

    def __init__(
        self,
        marketplace_region,
        report_name,
        report_options=None,
        ewah_options=None,
        *args,
        **kwargs,
    ):

        assert EWAHAmazonSellerCentralHook.validate_marketplace_region(
            marketplace_region, allow_lists=True
        ), f"Marketplace Region {marketplace_region} is invalid!"
        assert (
            report_name in EWAHAmazonSellerCentralHook._REPORT_METADATA.keys()
        ), "Invalid report name!"
        report_metadata = EWAHAmazonSellerCentralHook._REPORT_METADATA[report_name]
        kwargs["primary_key"] = report_metadata["primary_key"]
        kwargs["subsequent_field"] = report_metadata["subsequent_field"]

        # Update accepted extract strategies based on individual report
        for strategy in report_metadata.get("accepted_strategies", []):
            self._ACCEPTED_EXTRACT_STRATEGIES[strategy] = True

        super().__init__(*args, **kwargs)

        self.marketplace_region = marketplace_region
        self.report_name = report_name
        self.report_options = report_options
        self.ewah_options = ewah_options

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field) - (
                self.load_data_from_relative or timedelta(days=0)
            )
            data_until = self.data_until or datetime.utcnow().replace(tzinfo=pytz.utc)
        else:
            data_from = self.data_from
            data_until = self.data_until or datetime.utcnow().replace(tzinfo=pytz.utc)

        for batch in self.source_hook.get_data_from_reporting_api_in_batches(
            marketplace_region=self.marketplace_region,
            report_name=self.report_name,
            data_from=data_from,
            data_until=data_until,
            report_options=self.report_options,
            ewah_options=self.ewah_options,
        ):
            self.upload_data(batch)
