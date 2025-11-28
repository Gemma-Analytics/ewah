from ewah.constants import EWAHConstants as EC
from ewah.hooks.shopify_graphql import EWAHShopifyGraphQLHook
from ewah.operators.base import EWAHBaseOperator


class EWAHShopifyGraphQLOperator(EWAHBaseOperator):
    _NAMES = ["shopify_graphql"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_SUBSEQUENT: True,
    }

    def __init__(
        self,
        endpoint=None,
        shop_id=None,
        api_version=None,
        first=250,  # 250 is the max
        *args,
        **kwargs,
    ):
        self.endpoint = (endpoint or kwargs.get("target_table_name", "")).lower()

        assert (
            self.endpoint in EWAHShopifyGraphQLHook._ENDPOINTS
        ), f"Invalid endpoint '{self.endpoint}'! Valid endpoints: {', '.join(EWAHShopifyGraphQLHook._ENDPOINTS)}"

        # Set default subsequent_field for orders
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = kwargs.get("subsequent_field", "updatedAt")

        # Set default primary key
        kwargs["primary_key"] = kwargs.get(
            "primary_key", "gid"
        )  # ex: gid://shopify/{object_name}/{id}
        super().__init__(*args, **kwargs)
        self.shop_id = shop_id
        self.api_version = api_version
        self.first = first

    def ewah_execute(self, context):
        self._metadata.update({"shop_id": self.shop_id})

        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
            self.log.info(
                f"""Found max timestamp in result table -> Subsequent load:
                fetching orders updated after {data_from}"""
            )
        else:
            data_from = self.data_from

        for batch in self.source_hook.get_data(
            endpoint=self.endpoint,
            shop_id=self.shop_id,
            version=self.api_version,
            first=self.first,
            data_from=data_from,
        ):
            self.upload_data(batch)
