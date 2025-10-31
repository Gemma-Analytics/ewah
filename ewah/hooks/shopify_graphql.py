from ewah.hooks.base import EWAHBaseHook

import requests
import json


class EWAHShopifyGraphQLHook(EWAHBaseHook):
    _ATTR_RELABEL = {}

    # use same connection as in Rest API hook
    conn_name_attr = "ewah_shopify_grahql_conn_id"
    default_conn_name = "ewah_shopify_graphql_default"
    conn_type = "ewah_shopify_graphql"
    hook_name = "EWAH Shopify GraphQL Connection"

    DEFAULT_API_VERSION = "2023-10"
    _BASE_URL = "https://{shop}.myshopify.com/admin/api/{version}/graphql.json"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "host", "port", "schema"],
            "relabeling": {
                "login": "Shop Subdomain (https://{this_part}.myshopify.com)",
                "password": "Access Token",
            },
        }

    def execute_graphql_query(self, query, variables=None):
        """Execute a GraphQL query against Shopify's GraphQL API"""
        headers = {
            "X-Shopify-Access-Token": self.conn.password,
            "Content-Type": "application/json"
        }

        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = requests.post(
            self._BASE_URL.format(
                shop=self.conn.login,
                version=self.DEFAULT_API_VERSION,
            ),
            headers=headers,
            json=payload
        )
        response.raise_for_status()

        data = response.json()
        if "errors" in data:
            raise Exception(f"GraphQL Errors: {data['errors']}")

        return data.get("data")

    def flatten_order(self, order_node):
        """Flatten nested GraphQL order structure into a flat dictionary"""
        flattened = {}

        # Basic order fields
        flattened["id"] = order_node.get("id", "").split("/")[-1]  # Extract ID from GID
        flattened["gid"] = order_node.get("id")
        flattened["name"] = order_node.get("name")
        flattened["email"] = order_node.get("email")
        flattened["createdAt"] = order_node.get("createdAt")
        flattened["displayFinancialStatus"] = order_node.get("displayFinancialStatus")
        flattened["displayFulfillmentStatus"] = order_node.get("displayFulfillmentStatus")

        # Total price
        if order_node.get("totalPriceSet") and order_node["totalPriceSet"].get("shopMoney"):
            money = order_node["totalPriceSet"]["shopMoney"]
            flattened["totalPriceAmount"] = money.get("amount")
            flattened["totalPriceCurrencyCode"] = money.get("currencyCode")

        # Customer-> dump whole customer node in one column
        # note currently only selected ID in query
        customer_node = order_node.get("customer")
        # customer could be null for draft orders
        flattened["customer"] = json.dumps(customer_node) if customer_node else None

        # Line items - store as JSON since there can be multiple
        line_items = []
        if order_node.get("lineItems") and order_node["lineItems"].get("edges"):
            for edge in order_node["lineItems"]["edges"]:
                item = edge.get("node", {})
                item_data = {
                    "id": item.get("id", "").split("/")[-1] if item.get("id") else None,
                    "title": item.get("title"),
                    "quantity": item.get("quantity"),
                }
                if item.get("originalUnitPriceSet") and item["originalUnitPriceSet"].get("shopMoney"):
                    price = item["originalUnitPriceSet"]["shopMoney"]
                    item_data["originalUnitPriceAmount"] = price.get("amount")
                    item_data["originalUnitPriceCurrencyCode"] = price.get("currencyCode")
                line_items.append(item_data)
        flattened["lineItems"] = json.dumps(line_items) if line_items else None

        return flattened

    def get_data(
        self,
        shop_id=None,
        version=None,
        first=250,
    ):
        """Get orders data from Shopify GraphQL API with pagination"""
        shop_id = shop_id or self.conn.login
        version = version or self.DEFAULT_API_VERSION
        
        self._BASE_URL = self._BASE_URL.replace("{version}", version)

        # Hardcoded limit for testing purposes  -- TOD: remove latere
        max_rows = 2000
        
        query = """
        query getOrders($first: Int!, $after: String) {
            orders(first: $first, after: $after) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        id
                        name
                        email
                        createdAt
                        totalPriceSet {
                            shopMoney {
                                amount
                                currencyCode
                            }
                        }
                        displayFinancialStatus
                        displayFulfillmentStatus
                        customer {
                            id
                        }
                        lineItems(first: 250) {
                            edges {
                                node {
                                    title
                                    quantity
                                    originalUnitPriceSet {
                                        shopMoney {
                                            amount
                                            currencyCode
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        """

        has_next_page = True
        cursor = None

        ### REMOVE LATER
        total_rows = 0  # Initialize the counter
        #####


        while has_next_page:
            variables = {"first": first}
            if cursor:
                variables["after"] = cursor

            self.log.info(f"Fetching orders with cursor: {cursor}")

            #### REMOVE LATER
            self.log.info(f"In testing mode, fetching only 2000")
            self.log.info(f"total rows test: {total_rows}")
            ################

            data = self.execute_graphql_query(query, variables)

            if not data or "orders" not in data:
                break

            orders_data = data["orders"]
            page_info = orders_data["pageInfo"]
            orders = orders_data["edges"]

            # Flatten and yield batch
            flattened_orders = []
            for edge in orders:
                flattened_order = self.flatten_order(edge["node"])
                flattened_orders.append(flattened_order)

            # Increment BEFORE yield so the count is accurate
            total_rows += len(flattened_orders)

            if flattened_orders:
                yield flattened_orders

            ### Check max_rows limit after incrementing
            ### REMOVE LATER
            if total_rows >= max_rows:
                self.log.info(f"Reached max_rows limit of {max_rows}, stopping pagination")
                has_next_page = False
                break
            # #### REMOVE LATER

            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")