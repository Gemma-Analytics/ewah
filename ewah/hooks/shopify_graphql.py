from ewah.hooks.base import EWAHBaseHook

import requests
import json


class EWAHShopifyGraphQLHook(EWAHBaseHook):
    _ATTR_RELABEL = {}

    # Note: so far only query implemenation for orders node
    # including fields for lineItem JSON & discountApplication JSON

    conn_name_attr = "ewah_shopify_grahql_conn_id"
    default_conn_name = "ewah_shopify_graphql_default"
    conn_type = "ewah_shopify_graphql"
    hook_name = "EWAH Shopify GraphQL Connection"
    DEFAULT_API_VERSION = "2025-01"
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

    def execute_graphql_query(self, query, variables=None, shop=None, version=None):
        """Execute a GraphQL query against Shopify's GraphQL API"""
        headers = {
            "X-Shopify-Access-Token": self.conn.password,
            "Content-Type": "application/json"
        }

        payload = {"query": query}

        shop = shop or self.conn.login
        version = version or self.DEFAULT_API_VERSION

        if variables:
            payload["variables"] = variables

        response = requests.post(
            self._BASE_URL.format(
                shop=shop,
                version=version,
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

        flattened = {}

        # Basic order fields
        flattened["id"] = order_node.get("id", "").split("/")[-1]  # Extract ID from GID
        flattened["gid"] = order_node.get("id")
        flattened["name"] = order_node.get("name")
        flattened["email"] = order_node.get("email")
        flattened["createdAt"] = order_node.get("createdAt")
        flattened["updatedAt"] = order_node.get("updatedAt")
        flattened["cancelledAt"] = order_node.get("cancelledAt")
        flattened["processedAt"] = order_node.get("processedAt")
        flattened["closedAt"] = order_node.get("closedAt")
        flattened["currencyCode"] = order_node.get("currencyCode")
        flattened["note"] = order_node.get("note")
        flattened["tags"] = order_node.get("tags")
        flattened["totalWeight"] = order_node.get("totalWeight")
        flattened["displayFinancialStatus"] = order_node.get("displayFinancialStatus")
        flattened["displayFulfillmentStatus"] = order_node.get("displayFulfillmentStatus")
        flattened["statusPageUrl"] = order_node.get("statusPageUrl")
        flattened["taxesIncluded"] = order_node.get("taxesIncluded")
        flattened["currentSubtotalPriceSet"] = order_node.get("currentSubtotalPriceSet")
        flattened["totalPriceSet"] = order_node.get("totalPriceSet")
        flattened["totalTaxSet"] = order_node.get("totalTaxSet")
        flattened["totalDiscountsSet"] = order_node.get("totalDiscountsSet")
        flattened["discountCodes"] = order_node.get("discountCodes")
        # discount applications -> extract the node values
        disc_app_edges = order_node.get("discountApplications", {}).get("edges", [])
        disc_apps = [edge.get("node", {}) for edge in disc_app_edges]
        flattened["discountApplications"] = disc_apps

        customer_node = order_node.get("customer")
        # customer could be null for draft orders
        flattened["customer"] = json.dumps(customer_node) if customer_node else None

        # line items -> extract the node values
        line_item_edges = order_node.get("lineItems", {}).get("edges", [])
        line_items = [edge.get("node", {}) for edge in line_item_edges]
        flattened["lineItems"] = line_items

        return flattened

    def get_data(
        self,
        shop_id=None,
        version=None,
        first=50,
        data_from=None,
    ):
        """Get orders data from Shopify GraphQL API with pagination"""
        shop_id = shop_id or self.conn.login
        version = version or self.DEFAULT_API_VERSION
        
        # Remove testing limit
        max_rows = 200
        
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
        
        # Remove testing code
        total_rows = 0

        # Build query string for GraphQL if data_from is provided
        query_string = None
        if data_from:
            # Convert datetime to ISO format string for GraphQL
            if isinstance(data_from, str):
                # If it's already a string, try to parse it
                from dateutil.parser import parse
                data_from = parse(data_from)
            
            # Format as ISO 8601 string (required by Shopify GraphQL)
            if data_from.tzinfo is None:
                # If timezone-naive, assume UTC
                from pytz import UTC
                data_from = data_from.replace(tzinfo=UTC)
            
            iso_string = data_from.isoformat().replace('+00:00', 'Z')
            query_string = f"updated_at:>'{iso_string}'"
            self.log.info(f"Filtering orders with query: {query_string}")

        while has_next_page:
            variables = {"first": first}
            if cursor:
                variables["after"] = cursor
            if query_string:
                variables["query"] = query_string

            self.log.info(f"Fetching orders with cursor: {cursor}")

            # Remove testing code
            self.log.info(f"In testing mode, fetching only 200")
            self.log.info(f"total rows test: {total_rows}")

            data = self.execute_graphql_query(query, variables, shop=shop_id, version=version)

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

            # Log all updatedAt values for this batch
            updated_at_values = [order.get('updatedAt') for order in flattened_orders]
            self.log.info(f"Batch updatedAt values: {updated_at_values}")

            # Remove testing code
            total_rows += len(flattened_orders)

            if flattened_orders:
                yield flattened_orders

            # Remove testing code
            if total_rows >= max_rows:
                self.log.info(f"Reached max_rows limit of {max_rows}, stopping pagination")
                has_next_page = False
                break

            has_next_page = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")