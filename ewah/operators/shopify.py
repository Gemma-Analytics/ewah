from ewah.operators.base import EWAHBaseOperator
from ewah.utils.python_utils import is_iterable_not_string
from ewah.constants import EWAHConstants as EC

from ewah.hooks.base import EWAHBaseHook as BaseHook

from datetime import datetime, timedelta
from pytz import timezone

from requests.auth import HTTPBasicAuth
import requests
import json
import time
import copy


class EWAHShopifyOperator(EWAHBaseOperator):

    _NAMES = ["shopify"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: True,
    }

    _acceptable_api_versions = [
        "2020-07",
    ]
    _current_api_version = "2020-07"
    _base_url = "https://{shop}.myshopify.com/admin/api/{version}/{object}.json"

    # Set all accepted objects with their potential filter fields with defaults
    #   Note: always expected fields updated_at_max, updated_at_min, limit, id
    #   the key _is_drop_and_replace indicates objects that are only able
    #   to load with a full refresh
    _default_timestamp_fields = ("updated_at_min", "updated_at_max")
    _default_datetime_format = "%Y-%m-%d %H:%M:%S%z"
    _accepted_objects = {
        "balance_transactions": {
            "_is_drop_and_replace": True,
            "_object_url": "shopify_payments/balance/transactions",
            "_name_in_request_data": "transactions",
            "since_id": None,
            "last_id": None,
            "test": None,
            "payout_id": None,
            "payout_status": None,
        },
        "customers": {
            "ids": None,
            "since_id": None,
        },
        "disputes": {
            "_is_drop_and_replace": True,
            "_object_url": "shopify_payments/disputes",
        },
        "events": {
            "_timestamp_fields": ("created_at_min", "created_at_max"),
            "since_id": None,
            "filter": None,
            "verb": None,
        },
        "orders": {
            "ids": None,
            "since_id": None,
            "status": "any",
            "financial_status": None,
            "fulfillment_status": None,
            "fields": None,
        },
        "payouts": {
            "_timestamp_fields": ("date_min", "date_max"),
            "_datetime_format": "%Y-%m-%d",
            "_object_url": "shopify_payments/payouts",
            "since_id": None,
            "last_id": None,
            "status": None,
        },
        "products": {
            "ids": None,
            "since_id": None,
            "title": None,
            "vendor": None,
            "handle": None,
            "product_type": None,
            "collection_id": None,
            "published_status": None,
        },
        "tender_transactions": {
            "_timestamp_fields": ("processed_at_min", "processed_at_max"),
            "since_id": None,
        },
    }

    def __init__(
        self,
        shop_id,
        shopify_object,
        auth_type,
        filter_fields={},
        api_version=None,
        get_transactions_with_orders=False,
        get_events_with_orders=False,
        get_inventory_data_with_product_variants=False,
        page_limit=250,  # API Call pagination limit
        *args,
        **kwargs
    ):

        if is_iterable_not_string(shop_id):
            raise Exception("Multiple shops in one DAG is deprecated!")

        if get_transactions_with_orders and not shopify_object == "orders":
            raise Exception("transactions can only be pulled for orders!")

        if get_events_with_orders and not shopify_object == "orders":
            raise Exception("events can only be pulled for orders!")

        if (
            get_inventory_data_with_product_variants
            and not shopify_object == "products"
        ):
            raise Exception("inventory data may only be pulled with products!")

        if not shopify_object in self._accepted_objects.keys():
            raise Exception(
                "{0} is not in the list of accepted objects!"
                + " accepted objects: {1}".format(
                    shopify_object, ", ".join(self._accepted_objects.keys())
                )
            )

        if self._accepted_objects[shopify_object].get("_is_drop_and_replace"):
            kwargs["extract_strategy"] = EC.ES_FULL_REFRESH

        if not auth_type in ["access_token", "basic_auth"]:
            raise Exception("auth_type must be access_token or basic_auth!")

        if not type(filter_fields) == dict:
            raise Exception("filter_fields must be a dictionary!")
        else:
            for key, value in filter_fields.items():
                if not key in self._accepted_objects["shopify_object"].keys():
                    raise Exception(
                        "invalid key {0} in filter fields!".format(
                            key,
                        )
                    )

        api_version = api_version or self._current_api_version
        if not api_version in self._acceptable_api_versions:
            raise Exception(
                "{0} is not a valid api version! valid versions: {1}".format(
                    api_version,
                    ", ".join(self._acceptable_api_versions),
                )
            )

        if not type(page_limit) == int or page_limit > 250 or page_limit < 1:
            raise Exception("Page limit must be a positive integer not exceeding 250!")

        kwargs["primary_key"] = kwargs.get("primary_key", "id")

        # source conn id is not required on operator call level! avoid error
        kwargs["source_conn_id"] = kwargs.get("source_conn_id", "__none__")

        super().__init__(*args, **kwargs)

        self.shop_id = shop_id
        self.shopify_object = shopify_object
        self.auth_type = auth_type
        self.filter_fields = filter_fields
        self.api_version = api_version
        self.page_limit = page_limit
        self.get_transactions_with_orders = get_transactions_with_orders
        self.get_events_with_orders = get_events_with_orders
        self.get_inventory_data_with_product_variants = (
            get_inventory_data_with_product_variants
        )

    def ewah_execute(self, context):
        # can supply a list of shops - need to run for all shops individually!
        def datetime_to_string(dt, format):
            # check if tz aware; set to utc if so
            if dt.tzinfo:
                dt = dt.astimezone(timezone("UTC"))
            else:
                dt = dt.replace(tzinfo=timezone("UTC"))
            # check if format_string contains timezone
            if "%z" in format:
                # add colon!
                dt_string = dt.strftime(format)
                return dt_string[:-2] + ":" + dt_string[-2:]
            else:
                return dt.strftime(format)

        object_metadata = self._accepted_objects[self.shopify_object]
        self.object_metadata = object_metadata
        params = {
            key: val
            for key, val in object_metadata.items()
            if not val is None and not key[:1] == "_"
        }
        params.update(self.filter_fields)
        params["limit"] = self.page_limit

        timestamp_fields = object_metadata.get(
            "_timestamp_fields",
            self._default_timestamp_fields,
        )
        timestamp_format_string = object_metadata.get(
            "_datetime_format",
            self._default_datetime_format,
        )
        if self.data_until:
            params[timestamp_fields[1]] = datetime_to_string(
                self.data_until,
                timestamp_format_string,
            )
        if self.data_from:
            params[timestamp_fields[0]] = (
                datetime_to_string(
                    self.data_from,
                    timestamp_format_string,
                ),
            )

        source_conn_id = self.source_conn.conn_id
        auth_type = self.auth_type
        if is_iterable_not_string(self.shop_id):
            # multiple shops to iterate - loop through!
            # deprecated feature - don't use!
            raise Exception("Multiple Shops in one DAG is deactivated!")
            self.log.info("iterating through multiple shops!")
            for shop_id in self.shop_id:
                # metadata: shop id
                self.log.info("getting data for: {0}".format(shop_id))
                if hasattr(self.shop_id, "get") and hasattr(
                    self.shop_id[shop_id], "get"
                ):
                    # dict, not list! check for conn details!
                    shop_dict = self.shop_id[shop_id]
                    sci = shop_dict.get("source_conn_id", source_conn_id)
                    at = shop_dict.get("auth_type", auth_type)
                else:
                    sci = source_conn_id
                    at = auth_type
                self._metadata.update({"shop_id": shop_id})
                self.execute_for_shop(context, shop_id, params, sci, at)
        else:
            self._metadata.update({"shop_id": self.shop_id})
            sci = self.source_conn.conn_id
            at = self.auth_type
            self.execute_for_shop(context, self.shop_id, params, sci, at)

    def execute_for_shop(
        self,
        context,
        shop_id,
        params,
        source_conn_id,
        auth_type,
    ):
        # Get data from shopify via REST API
        def add_get_transactions(data, shop, version, req_kwargs):
            # workaround to add transactions to orders
            self.log.info("Requesting transactions of orders...")
            base_url = "https://{shop}.myshopify.com/admin/api/{version}/orders/{id}/transactions.json"
            base_url = base_url.format(
                **{
                    "shop": shop,
                    "version": version,
                    "id": "{id}",
                }
            )

            for datum in data:
                id = datum["id"]
                # self.log.info('getting transactions for order {0}'.format(id))
                time.sleep(1)  # avoid hitting api call requested per second limit
                url = base_url.format(id=id)
                req = requests.get(url, **req_kwargs)
                if not req.status_code == 200:
                    self.log.info("response: " + str(req.status_code))
                    self.log.info("request text: " + req.text)
                    raise Exception("non-200 response!")
                transactions = json.loads(req.text).get("transactions", [])
                datum["transactions"] = transactions

            return data

        def add_get_inventoryitems(data, shop, version, req_kwargs):
            # workaround to get inventory item data (i.e. costs) for products
            self.log.info("Requesting inventory items of product variants...")
            base_url = (
                "https://{shop}.myshopify.com/admin/api/{version}/inventory_items.json"
            )
            url = base_url.format(
                shop=shop,
                version=version,
            )

            kwargs = copy.deepcopy(req_kwargs)

            for datum in data:
                ids = [v["inventory_item_id"] for v in datum.get("variants", [])]
                if ids:
                    kwargs["params"] = {"ids": copy.deepcopy(ids)}
                    time.sleep(1)  # avoid hitting api call requested limit
                    req = requests.get(url, **kwargs)
                    if not req.status_code == 200:
                        self.log.info("response: " + str(req.status_code))
                        self.log.info("request text: " + req.text)
                        raise Exception("non-200 response!")
                    inv_items = json.loads(req.text).get("inventory_items", [])
                    datum["inventory_items"] = inv_items

            return data

        def add_get_events(data, shop, version, req_kwargs):
            # workaround to add events of an order to orders
            self.log.info("Requesting events of orders...")
            base_url = "https://{shop}.myshopify.com/admin/api/{version}/orders/{id}/events.json"
            base_url = base_url.format(
                shop=shop,
                version=version,
                id="{id}",
            )

            for datum in data:
                id = datum["id"]
                time.sleep(1)
                url = base_url.format(id=id)
                req = requests.get(url, **req_kwargs)
                if not req.status_code == 200:
                    self.log.info("response: " + str(req.status_code))
                    self.log.info("request text: " + req.text)
                    raise Exception("non-200 response!")
                events = json.loads(req.text).get("events", [])
                datum["events"] = events

            return data

        url = self._base_url.format(
            **{
                "shop": shop_id,
                "version": self.api_version,
                "object": self.object_metadata.get(
                    "_object_url",
                    self.shopify_object,
                ),
            }
        )

        # get connection for the applicable shop
        conn = BaseHook.get_connection(source_conn_id)
        login = conn.login
        password = conn.password

        if auth_type == "access_token":
            headers = {
                "X-Shopify-Access-Token": password,
            }
            kwargs_init = {
                "headers": headers,
                "params": params,
            }
            kwargs_links = {"headers": headers}
        elif auth_type == "basic_auth":
            kwargs_init = {
                "params": params,
                "auth": HTTPBasicAuth(login, password),
            }
            kwargs_links = {"auth": HTTPBasicAuth(login, password)}
        else:
            raise Exception("Authentication type not accepted!")

        # get and upload data
        self.log.info(
            "Requesting data from REST API - url: {0}, params: {1}".format(
                url, str(params)
            )
        )
        req_kwargs = kwargs_init
        is_first = True
        while is_first or (r.status_code == 200 and url):
            r = requests.get(url, **req_kwargs)
            if is_first:
                is_first = False
                req_kwargs = kwargs_links
            data = json.loads(r.text or "{}").get(
                self.object_metadata.get(
                    "_name_in_request_data",
                    self.shopify_object,
                )
            )
            if self.get_transactions_with_orders:
                data = add_get_transactions(
                    data=data,
                    shop=shop_id,
                    version=self.api_version,
                    req_kwargs=kwargs_links,
                )
            if self.get_events_with_orders:
                data = add_get_events(
                    data=data,
                    shop=shop_id,
                    version=self.api_version,
                    req_kwargs=kwargs_links,
                )
            if self.get_inventory_data_with_product_variants:
                data = add_get_inventoryitems(
                    data=data,
                    shop=shop_id,
                    version=self.api_version,
                    req_kwargs=kwargs_links,
                )
            self.upload_data(data)
            self.log.info("Requesting next page of data...")
            if r.headers.get("Link") and r.headers["Link"][-9:] == 'el="next"':
                url = r.headers["Link"][1:-13]
            else:
                url = None

        if not r.status_code == 200:
            raise Exception(
                "Shopify request returned an error {1}: {0}".format(
                    r.text,
                    str(r.status_code),
                )
            )
