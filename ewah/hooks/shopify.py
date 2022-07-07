from ewah.hooks.base import EWAHBaseHook

from pytz import timezone
import requests
import time
import copy
import dateutil


class EWAHShopifyHook(EWAHBaseHook):

    _ATTR_RELABEL = {}

    conn_name_attr = "ewah_shopify_conn_id"
    default_conn_name = "ewah_shopify_default"
    conn_type = "ewah_shopify"
    hook_name = "EWAH Shopify Connection"

    DEFAULT_API_VERSION = "2022-01"
    _BASE_URL = "https://{shop}.myshopify.com/admin/api/{version}/{object}.json"

    _DEFAULT_TIMESTAMP_FIELDS = ("updated_at_min", "updated_at_max", "updated_at")
    _DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S%z"
    _OBJECTS = {
        # "object_name": {
        #   "_is_drop_and_replace": True, - set if loading possible only as full refresh
        #   "_object_url": "string", - set if differs from object_name
        #   "_name_in_request_data", - set if differs from object_name
        #   "_timestamp_fields", - set if differs from defaults
        #   "_datetime_format", - set if differ from defaults
        # },
        "balance_transactions": {
            "_is_drop_and_replace": True,
            "_object_url": "shopify_payments/balance/transactions",
            "_name_in_request_data": "transactions",
        },
        "customers": {},
        "disputes": {
            "_is_drop_and_replace": True,
            "_object_url": "shopify_payments/disputes",
        },
        "events": {
            "_timestamp_fields": ("created_at_min", "created_at_max", "created_at"),
        },
        "inventory_levels": {},
        "marketing_events": {},
        "orders": {},
        "draft_orders": {},
        "locations": {},
        "checkouts": {},  # Same as abandoned_checkouts
        "abandoned_checkouts": {
            "_object_url": "checkouts",
            "_name_in_request_data": "checkouts",
        },
        "payouts": {
            "_timestamp_fields": ("date_min", "date_max", "date"),
            "_datetime_format": "%Y-%m-%d",
            "_object_url": "shopify_payments/payouts",
        },
        "price_rules": {},
        "products": {},
        "tender_transactions": {
            "_timestamp_fields": (
                "processed_at_min",
                "processed_at_max",
                "processed_at",
            ),
        },
    }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "host", "port", "schema"],
            "relabeling": {
                "login": "Shop Subdomain (https://{this_part}.myshopify.com)",
                "password": "Access Token",
            },
        }

    def add_get_transactions(self, data, shop, version, req_kwargs):
        # Adds transactions to orders
        self.log.info("Requesting transactions of orders...")
        base_url = self._BASE_URL.format(
            shop=shop,
            version=version,
            object="orders/{id}/transactions",
        )

        for datum in data:
            id = datum["id"]
            time.sleep(1)  # avoid hitting api call requests per second limit
            url = base_url.format(id=id)
            response = requests.get(url, **req_kwargs)
            assert response.status_code == 200, "Code {0}: {1}".format(
                response.status_code, response.text
            )
            datum["transactions"] = response.json().get("transactions", [])

        return data

    def add_get_inventoryitems(self, data, shop, version, req_kwargs):
        # Adds inventory item data (i.e. costs) for products
        self.log.info("Requesting inventory items of product variants...")
        url = self._BASE_URL.format(
            shop=shop,
            version=version,
            object="inventory_items",
        )
        kwargs = copy.deepcopy(req_kwargs)

        for datum in data:
            ids = [v["inventory_item_id"] for v in datum.get("variants", [])]
            if ids:
                kwargs["params"] = {"ids": copy.deepcopy(ids)}
                time.sleep(1)  # avoid hitting api call requested limit
                response = requests.get(url, **kwargs)
                assert response.status_code == 200, "Code {0}: {1}".format(
                    response.status_code, response.text
                )
                datum["inventory_items"] = response.json().get("inventory_items", [])

        return data

    def add_get_events(data, shop, version, req_kwargs):
        # Adds events of an order to orders
        self.log.info("Requesting events of orders...")
        base_url = self._BASE_URL.format(
            shop=shop,
            version=version,
            object="orders/{id}/events",
        )

        for datum in data:
            id = datum["id"]
            time.sleep(1)
            url = base_url.format(id=id)
            response = requests.get(url, **req_kwargs)
            assert response.status_code == 200, "Code {0}: {1}".format(
                response.status_code, response.text
            )
            datum["events"] = response.json().get("events", [])

        return data

    @staticmethod
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

    def get_data(
        self,
        shopify_object,
        filter_fields,
        shop_id=None,
        version=None,
        data_from=None,
        data_until=None,
        add_transactions=False,
        add_events=False,
        add_inventoryitems=False,
    ):
        # Get data from Shopify via REST API
        assert shopify_object in self._OBJECTS.keys(), "Object invalid!"
        object_metadata = self._OBJECTS[shopify_object]

        version = version or self.DEFAULT_API_VERSION
        shop_id = shop_id or self.conn.login
        url = self._BASE_URL.format(
            shop=shop_id,
            version=version,
            object=object_metadata.get(
                "_object_url",
                shopify_object,
            ),
        )

        params = {"limit": 250}
        params.update(filter_fields)
        if shopify_object == "orders":
            # default the status param for orders to any
            params["status"] = params.get("status", "any")
        timestamp_fields = object_metadata.get(
            "_timestamp_fields",
            self._DEFAULT_TIMESTAMP_FIELDS,
        )
        timestamp_format_string = object_metadata.get(
            "_datetime_format",
            self._DEFAULT_DATETIME_FORMAT,
        )
        if data_from:
            params[timestamp_fields[0]] = self.datetime_to_string(
                data_from,
                timestamp_format_string,
            )
        if data_until:
            params[timestamp_fields[1]] = self.datetime_to_string(
                data_until,
                timestamp_format_string,
            )

        headers = {
            "X-Shopify-Access-Token": self.conn.password,
        }

        # for endpoints that need ids
        ids_list = []
        if shopify_object == "inventory_levels":
            for chunk in self.get_data(
                shopify_object="locations",
                filter_fields={},
                shop_id=shop_id,
                version=version,
                data_from=None,
                data_until=None,
                add_transactions=False,
                add_events=False,
                add_inventoryitems=False,
            ):
                for location in chunk:
                    ids_list.append(location["id"])

        kwargs_init = {
            "headers": headers,
            "params": params,
        }
        kwargs_links = {"headers": headers}

        self.log.info(
            "Requesting data from REST API - url: {0}, params: {1}".format(
                url, str(params)
            )
        )
        req_kwargs = kwargs_init
        is_first = True
        finished_pagination = True
        while is_first or response.status_code == 200:
            if shopify_object == "inventory_levels" and (
                is_first or finished_pagination
            ):
                # inventory_levels endpoint only takes 50 ids max at a time
                location_ids = ",".join([str(id) for id in ids_list[:50]])
                ids_list = ids_list[50:]
                req_kwargs["params"]["location_ids"] = location_ids
                # if there are more ids than the allowed limit we need to restart
                # the request with new ids once the previous pagination is done
                finished_pagination = False

            response = requests.get(url, **req_kwargs)
            if is_first or not finished_pagination:
                is_first = False
                req_kwargs = kwargs_links
            data = response.json().get(
                object_metadata.get(
                    "_name_in_request_data",
                    shopify_object,
                )
            )
            if add_transactions:
                data = self.add_get_transactions(
                    data=data,
                    shop=shop_id,
                    version=version,
                    req_kwargs=kwargs_links,
                )
            if add_events:
                data = self.add_get_events(
                    data=data,
                    shop=shop_id,
                    version=version,
                    req_kwargs=kwargs_links,
                )
            if add_inventoryitems:
                data = self.add_get_inventoryitems(
                    data=data,
                    shop=shop_id,
                    version=version,
                    req_kwargs=kwargs_links,
                )

            if data and not object_metadata.get("_is_drop_and_replace", False):
                for datum in data:
                    datum[timestamp_fields[2]] = dateutil.parser.parse(
                        datum[timestamp_fields[2]]
                    )
            yield data

            if response.headers.get("Link") and response.headers["Link"].endswith(
                'el="next"'
            ):
                self.log.info("Requesting next page of data...")
                url = response.headers["Link"][1:-13]
            elif ids_list and shopify_object == "inventory_levels":
                # after pagination complete we restart the requests while
                # we still have ids in id_list
                finished_pagination = True
                url = self._BASE_URL.format(
                    shop=shop_id,
                    version=version,
                    object="inventory_levels",
                )
                req_kwargs = kwargs_init
            else:
                break

        assert response.status_code == 200, "Code {0}: {1}".format(
            response.status_code, response.text
        )
