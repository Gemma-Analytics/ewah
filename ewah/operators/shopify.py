from ewah.constants import EWAHConstants as EC
from ewah.hooks.shopify import EWAHShopifyHook
from ewah.operators.base import EWAHBaseOperator
from ewah.utils.python_utils import is_iterable_not_string


class EWAHShopifyOperator(EWAHBaseOperator):
    _NAMES = ["shopify"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    def __init__(
        self,
        shopify_object=None,
        shop_id=None,
        filter_fields=None,
        api_version=None,
        get_transactions_with_orders=False,
        get_events_with_orders=False,
        get_inventory_data_with_product_variants=False,
        *args,
        **kwargs
    ):
        shopify_object = shopify_object or kwargs["target_table_name"]

        if is_iterable_not_string(shop_id):
            raise Exception("Multiple shops in one DAG is not allowed anymore!")

        if get_transactions_with_orders and not shopify_object == "orders":
            raise Exception("transactions can only be pulled for orders!")

        if get_events_with_orders and not shopify_object == "orders":
            raise Exception("events can only be pulled for orders!")

        if (
            get_inventory_data_with_product_variants
            and not shopify_object == "products"
        ):
            raise Exception("inventory data may only be pulled with products!")

        assert (
            shopify_object in EWAHShopifyHook._OBJECTS.keys()
        ), "Object not implemented!"

        filter_fields = filter_fields or {}
        if not isinstance(filter_fields, dict):
            raise Exception("filter_fields must be a dictionary!")

        kwargs["primary_key"] = kwargs.get("primary_key", "id")
        if (
            shopify_object in ("checkouts", "abandoned_checkouts")
            and kwargs["primary_key"] == "id"
        ):
            # Special case: for abandoned checkouts, the PK is "token" and not "id"
            # The id refers to the checkout id, which can be abandoned multiple times
            # Each abandonement has a unique token, but carries the same id
            kwargs["primary_key"] = "token"
        if EWAHShopifyHook._OBJECTS[shopify_object].get("_is_drop_and_replace"):
            kwargs["extract_strategy"] = EC.ES_FULL_REFRESH
        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = EWAHShopifyHook._OBJECTS[shopify_object].get(
                "_timestamp_fields",
                EWAHShopifyHook._DEFAULT_TIMESTAMP_FIELDS,
            )[2]

        super().__init__(*args, **kwargs)

        self.shop_id = shop_id
        self.shopify_object = shopify_object
        self.filter_fields = filter_fields
        self.api_version = api_version
        self.get_transactions_with_orders = get_transactions_with_orders
        self.get_events_with_orders = get_events_with_orders
        self.get_inventory_data_with_product_variants = (
            get_inventory_data_with_product_variants
        )

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
            data_until = None
        else:
            data_from = self.data_from
            data_until = self.data_until

        self._metadata.update({"shop_id": self.shop_id})
        for batch in self.source_hook.get_data(
            shop_id=self.shop_id,
            filter_fields=self.filter_fields,
            shopify_object=self.shopify_object,
            version=self.api_version,
            data_from=data_from,
            data_until=data_until,
            add_transactions=self.get_transactions_with_orders,
            add_events=self.get_events_with_orders,
            add_inventoryitems=self.get_inventory_data_with_product_variants,
        ):
            self.upload_data(batch)
