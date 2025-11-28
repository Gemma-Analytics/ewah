from ewah.hooks.base import EWAHBaseHook

import requests
import json
import time
from dateutil.parser import parse
from pytz import UTC


class EWAHShopifyGraphQLHook(EWAHBaseHook):
    _ATTR_RELABEL = {}

    # Note: so far only query implemenation for orders node
    # with line Items as JSON column

    conn_name_attr = "ewah_shopify_graphql_conn_id"
    default_conn_name = "ewah_shopify_graphql_default"
    conn_type = "ewah_shopify_graphql"
    hook_name = "EWAH Shopify GraphQL Connection"
    DEFAULT_API_VERSION = "2025-10"
    _BASE_URL = "https://{shop}.myshopify.com/admin/api/{version}/graphql.json"

    _ENDPOINTS = [
        "orders",
    ]

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "host", "port", "schema"],
            "relabeling": {
                "login": "Shop Subdomain (https://{this_part}.myshopify.com)",
                "password": "Access Token",
            },
        }

    def execute_graphql_query(
        self, query, variables=None, shop=None, version=None, max_retries=3
    ):
        """Execute a GraphQL query against Shopify's GraphQL API with retry logic"""
        headers = {
            "X-Shopify-Access-Token": self.conn.password,
            "Content-Type": "application/json",
            "X-GraphQL-Cost-Include-Fields": "true",
        }

        payload = {"query": query}

        shop = shop or self.conn.login
        version = version or self.DEFAULT_API_VERSION

        if variables:
            payload["variables"] = variables

        retry_delay = 1.0
        for attempt in range(max_retries + 1):
            try:
                response = requests.post(
                    self._BASE_URL.format(
                        shop=shop,
                        version=version,
                    ),
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()

                data = response.json()

                # Log cost information if available:
                # (1) requestedQueryCost: estimated before execution
                # (2) actualQueryCost: real cost after execution (max 1,000 points allowed)
                #      Note: "Calls to the GraphQL Admin API are limited based on calculated query
                #      costs, which means you should consider the cost of requests over time,
                #      rather than the number of requests.
                # (3) currentlyAvailable: leaky bucket capacity,
                #      see https://shopify.dev/docs/api/usage/limits#the-leaky-bucket-algorithm
                if "extensions" in data and "cost" in data["extensions"]:
                    cost = data["extensions"]["cost"]
                    self.log.info(
                        f"Query cost - Requested: {cost.get('requestedQueryCost')}, "
                        f"Actual: {cost.get('actualQueryCost')}, "
                        f"Available: {cost.get('throttleStatus', {}).get('currentlyAvailable')}"
                    )

                if "errors" in data:
                    raise Exception(f"GraphQL Errors: {data['errors']}")

                return data.get("data")

            except requests.exceptions.RequestException as e:
                if attempt == max_retries:
                    raise e
                self.log.warning(
                    f"Request failed (attempt {attempt + 1}/{max_retries + 1}): {e}"
                )
                time.sleep(retry_delay * (2**attempt))

    def flatten_order(self, order_node):

        flattened = {}

        # Basic order fields
        flattened["id"] = order_node.get("id", "").split("/")[-1]  # Extract ID from GID
        flattened["legacyResourceId"] = order_node.get("legacyResourceId")
        flattened["gid"] = order_node.get("id")
        flattened["name"] = order_node.get("name")
        flattened["number"] = order_node.get("number")
        flattened["email"] = order_node.get("email")
        flattened["createdAt"] = order_node.get("createdAt")
        flattened["updatedAt"] = order_node.get("updatedAt")
        flattened["processedAt"] = order_node.get("processedAt")
        flattened["cancelledAt"] = order_node.get("cancelledAt")
        flattened["cancelReason"] = order_node.get("cancelReason")
        flattened["closedAt"] = order_node.get("closedAt")
        flattened["currencyCode"] = order_node.get("currencyCode")
        flattened["customerLocale"] = order_node.get("customerLocale")
        flattened["note"] = order_node.get("note")
        flattened["tags"] = order_node.get("tags")
        flattened["totalWeight"] = order_node.get("totalWeight")
        flattened["displayFinancialStatus"] = order_node.get("displayFinancialStatus")
        flattened["displayFulfillmentStatus"] = order_node.get(
            "displayFulfillmentStatus"
        )
        flattened["sourceName "] = order_node.get("sourceName ")
        flattened["returnStatus"] = order_node.get("returnStatus")
        flattened["statusPageUrl"] = order_node.get("statusPageUrl")
        flattened["taxesIncluded"] = order_node.get("taxesIncluded")
        flattened["currentSubtotalPriceSet"] = order_node.get("currentSubtotalPriceSet")
        flattened["totalPriceSet"] = order_node.get("totalPriceSet")
        flattened["totalTaxSet"] = order_node.get("totalTaxSet")
        flattened["totalDiscountsSet"] = order_node.get("totalDiscountsSet")
        flattened["discountCodes"] = order_node.get("discountCodes")

        # discountApplications -> extract the node values
        disc_app_edges = order_node.get("discountApplications", {}).get("edges", [])
        disc_apps = [edge.get("node", {}) for edge in disc_app_edges]
        flattened["discountApplications"] = disc_apps

        # customer - extract (no egdes)
        customer_node = order_node.get("customer")
        flattened["customer"] = json.dumps(customer_node) if customer_node else None

        # lineItems -> extract the node values
        line_item_edges = order_node.get("lineItems", {}).get("edges", [])
        line_items = [edge.get("node", {}) for edge in line_item_edges]
        flattened["lineItems"] = line_items

        return flattened

    def get_data(
        self,
        endpoint,
        shop_id=None,
        version=None,
        first=50,
        data_from=None,
    ):
        """Get data from Shopify GraphQL API with pagination
        
        Note on pagination limits:
        - Orders: max 250 per page (controlled by 'first' parameter, default 50)
        - Line items: max 250 per order (hardcoded in query)
        - Discount applications: max 250 per order (hardcoded in query)
        
        For cases where we expect more items than these limits (e.g., orders with >250 line items,
        or orders with >250 discount applications), additional pagination logic would be needed
        to fetch subsequent pages using cursor-based pagination.
        """
        assert (
            endpoint in self._ENDPOINTS
        ), f"Invalid endpoint '{endpoint}'! Valid endpoints: {', '.join(self._ENDPOINTS)}"
        # Note: Currently only "orders" endpoint is implemented
        assert (
            endpoint == "orders"
        ), f"Endpoint '{endpoint}' not yet implemented. Only 'orders' is supported."

        shop_id = shop_id or self.conn.login
        version = version or self.DEFAULT_API_VERSION

        query = """
        query getOrders($first: Int!, $after: String, $query: String) {
            orders(first: $first, after: $after, query: $query) {
                pageInfo {
                    hasNextPage
                    endCursor
                }
                edges {
                    node {
                        id
                        legacyResourceId
                        name
                        number
                        email
                        createdAt
                        updatedAt
                        processedAt
                        closedAt
                        cancelledAt
                        cancelReason
                        currencyCode
                        customer {
                            id
                            emailMarketingConsent {
                                marketingState
                                marketingOptInLevel
                                consentUpdatedAt
                            }
                        }
                        customerLocale
                        taxesIncluded
                        currentSubtotalPriceSet { shopMoney { amount currencyCode } }
                        totalPriceSet { shopMoney { amount currencyCode } }
                        totalTaxSet { shopMoney { amount currencyCode } }
                        totalDiscountsSet { shopMoney { amount currencyCode } }
                        discountCodes
                        discountApplications(first: 250) {
                            edges {
                                node {
                                __typename
                                index
                                targetSelection
                                targetType
                                allocationMethod

                                ... on DiscountCodeApplication {
                                    code
                                }
                                ... on AutomaticDiscountApplication { title }
                                ... on ManualDiscountApplication { title description }
                                ... on ScriptDiscountApplication { title }

                                value {
                                    __typename
                                    ... on MoneyV2 { amount currencyCode }
                                    ... on PricingPercentageValue { percentage }
                                }
                                }
                            }
                            }

                        displayFinancialStatus
                        displayFulfillmentStatus
                        note
                        tags
                        totalWeight
                        statusPageUrl
                        returnStatus
                        sourceName

                        lineItems(first: 250) {
                            edges {
                                node {
                                    id
                                    name
                                    title
                                    quantity
                                    refundableQuantity
                                    sku
                                    customAttributes { key value }
                                    totalDiscountSet { shopMoney { amount currencyCode }}
                                    originalUnitPriceSet { shopMoney { amount currencyCode }}
                                    taxLines {
                                        title
                                        rate
                                        priceSet {
                                            shopMoney { amount currencyCode }
                                        }
                                    }
                                    discountAllocations {
                                        allocatedAmountSet { shopMoney { amount currencyCode } }
                                        discountApplication { index }
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

        # Build query string for GraphQL if data_from provided
        query_string = None
        if data_from:
            # ensure UTC timezone and ISO 8601 format
            dt = parse(str(data_from))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)

            iso_string = dt.isoformat().replace("+00:00", "Z")
            query_string = f"updated_at:>='{iso_string}'"
            self.log.info(f"Filtering orders with query: {query_string}")

        while has_next_page:
            variables = {"first": first}
            if cursor:
                variables["after"] = cursor
            if query_string:
                variables["query"] = query_string

            self.log.info(f"Fetching orders with cursor: {cursor}")

            data = self.execute_graphql_query(
                query, variables, shop=shop_id, version=version
            )

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

            if flattened_orders:
                yield flattened_orders

            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")
