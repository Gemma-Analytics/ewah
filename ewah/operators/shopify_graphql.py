from ewah.constants import EWAHConstants as EC
from ewah.hooks.shopify_graphql import EWAHShopifyGraphQLHook
from ewah.operators.base import EWAHBaseOperator


class EWAHShopifyGraphQLOperator(EWAHBaseOperator):
    _NAMES = ["shopify_graphql"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
    }

    def __init__(
        self,
        shop_id=None,
        api_version=None,
        first=250,
        *args,
        **kwargs
    ):
        # Only allow full refresh for now
        if kwargs.get("extract_strategy") and kwargs["extract_strategy"] != EC.ES_FULL_REFRESH:
            raise Exception("Only full refresh is supported for Shopify GraphQL connector")
        kwargs["extract_strategy"] = EC.ES_FULL_REFRESH

        # Set default primary key
        kwargs["primary_key"] = kwargs.get("primary_key", "id")

        super().__init__(*args, **kwargs)

        self.shop_id = shop_id
        self.api_version = api_version
        self.first = first


    def ewah_execute(self, context):
        self._metadata.update({"shop_id": self.shop_id})

        for batch in self.source_hook.get_data(
            shop_id=self.shop_id,
            version=self.api_version,
            first=self.first,
        ):
            self.upload_data(batch)