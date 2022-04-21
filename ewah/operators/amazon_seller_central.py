from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator
from ewah.hooks.amazon_seller_central import EWAHAmazonSellerCentralHook

from datetime import datetime


class EWAHAmazonSellerCentralOperator(EWAHBaseOperator):

    _NAMES = ["asc", "seller_central", "amazon_seller_central", "sp-api"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: True,
    }

    def __init__(
        self, marketplace_region, resource=None, sideloads=None, *args, **kwargs
    ):
        self.resource = resource or kwargs["target_table_name"]
        self.marketplace_region = marketplace_region
        if sideloads:
            if isinstance(sideloads, str):
                self.sideloads = [sideloads]
            else:
                assert isinstance(sideloads, list), "sideloads must be str or list!"
                self.sideloads = sideloads
        else:
            self.sideloads = None

        assert EWAHAmazonSellerCentralHook.validate_marketplace_region(
            marketplace_region
        ), f"Marketplace Region {marketplace_region} is invalid!"

        metadata = EWAHAmazonSellerCentralHook._APIS[self.resource]
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = metadata["datetime_filter_field"]
        kwargs["primary_key"] = metadata["primary_key"]

        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
        else:
            data_from = self.data_from

        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            marketplace_region=self.marketplace_region,
            since_date=data_from,
            sideloads=self.sideloads,
        ):
            self.upload_data(batch)
