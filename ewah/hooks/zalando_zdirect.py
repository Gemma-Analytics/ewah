"""Zalando zDirect API Connector

API docs: https://developers.merchants.zalando.com/docs/index.html
"""

from ewah.hooks.base import EWAHBaseHook

import base64
import time
import requests


class EWAHZalandoZDirectHook(EWAHBaseHook):
    """
    Hook to communicate with the Zalando zDirect API.
    Uses OAuth 2.0 client credentials flow for authentication.

    Connection fields:
    - login: client_id
    - password: client_secret
    - host: merchant_id

    The API base URL defaults to production. The operator overrides it via its
    `base_url` kwarg (set `base_url` in the DAG's `general_config` to point at
    sandbox).
    """

    _ATTR_RELABEL = {
        "client_id": "login",
        "client_secret": "password",
        "merchant_id": "host",
    }

    conn_name_attr = "ewah_zalando_zdirect_conn_id"
    default_conn_name = "ewah_zalando_zdirect_default"
    conn_type = "ewah_zalando_zdirect"
    hook_name = "EWAH Zalando zDirect Connection"

    # Default; the operator overrides this per instance (e.g. to sandbox)
    base_url = "https://api.merchants.zalando.com"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.access_token = None
        self.token_expires_at = 0

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra"],
            "relabeling": {
                "login": "Client ID",
                "password": "Client Secret",
                "host": "Merchant ID",
            },
        }

    @property
    def session(self):
        # Reuse a single TCP+TLS connection across all API calls; saves the handshake
        # cost on tens of thousands of per-order/per-item requests during full load.
        if not hasattr(self, "_session"):
            self._session = requests.Session()
        return self._session

    def close(self):
        # Called by EWAHBaseOperator at the end of execute()
        if hasattr(self, "_session"):
            self._session.close()
            del self._session

    @property
    def merchant_id(self):
        """Get merchant ID from connection host field."""
        return self.conn.merchant_id

    def authenticate(self):
        """
        Authenticate using OAuth 2.0 client credentials flow.
        Returns headers with Bearer token.
        """
        time_now = time.time()

        # Reuse token if still valid (with 60 second buffer)
        if self.access_token and time_now < (self.token_expires_at - 60):
            return {
                "Authorization": f"Bearer {self.access_token}",
                "Accept": "application/json",
            }

        # Get credentials from Airflow connection
        client_id = self.conn.client_id
        client_secret = self.conn.client_secret

        # Encode credentials for Basic Auth header
        credentials = f"{client_id}:{client_secret}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()

        token_url = f"{self.base_url}/auth/token"
        token_headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        token_data = {
            "grant_type": "client_credentials",
            "scope": "access_token_only",
        }

        self.log.info("Requesting OAuth token from Zalando zDirect API...")

        # Retry logic for rate limiting (429 errors)
        max_retries = 5
        retry_delay = 12

        for attempt in range(max_retries):
            response = self.session.post(token_url, headers=token_headers, data=token_data)

            if response.status_code == 200:
                break
            elif response.status_code == 429:
                # Rate Limiting docs https://developers.merchants.zalando.com/docs/rate-limiting.html
                wait_time = retry_delay * (2 ** attempt)
                self.log.warning(f"Too many requests. For more information please check Rate Limiting from developer docs. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                raise Exception(f"Failed to obtain access token: {response.status_code} - {response.text}")

        if response.status_code != 200:
            raise Exception(f"Failed to obtain access token after {max_retries} retries: {response.status_code} - {response.text}")

        token_response = response.json()
        self.access_token = token_response.get("access_token")
        expires_in = token_response.get("expires_in", 3600)  # Default 1 hour
        self.token_expires_at = time_now + expires_in

        self.log.info(f"Successfully obtained OAuth token (expires in {expires_in}s)")

        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

    def build_endpoint_url(self, endpoint, order_id=None, order_item_id=None):
        """
        Build the full endpoint URL based on the endpoint type.
        Kinda manual but with the number of endpoints being small we can affort it.
        """
        if endpoint == "orders":
            return f"{self.base_url}/merchants/{self.merchant_id}/orders"
        elif endpoint == "order-items":
            if order_id:
                return f"{self.base_url}/merchants/{self.merchant_id}/orders/{order_id}/items"
            else:
                # Return orders URL to fetch order IDs first
                return f"{self.base_url}/merchants/{self.merchant_id}/orders"
        elif endpoint == "order-item-lines":
            if order_id and order_item_id:
                return f"{self.base_url}/merchants/{self.merchant_id}/orders/{order_id}/items/{order_item_id}/lines"
            else:
                # Return orders URL to fetch order IDs first
                return f"{self.base_url}/merchants/{self.merchant_id}/orders"
        elif endpoint == "customer-returned-items":
            return f"{self.base_url}/zrs/merchants/{self.merchant_id}/customer-returned-items"
        elif endpoint == "item-quantity-snapshots":
            return f"{self.base_url}/zfs/item-quantity-snapshots/{self.merchant_id}"
        else:
            raise ValueError(f"Unknown endpoint: {endpoint}")

    def fetch_paginated_data(self, base_url, params=None, page_size=50):
        """
        Fetch data with pagination support.
        Zalando uses page[size] and page[cursor] for pagination.
        """
        if params is None:
            params = {}

        params["page[size]"] = page_size
        self.log.info(f"Using page size: {page_size}")
        url = base_url

        while url:
            headers = self.authenticate()

            self.log.info(f"Fetching data from: {url}")

            # Retry logic for rate limiting
            max_retries = 6
            retry_delay = 4

            for attempt in range(max_retries):
                response = self.session.get(url, headers=headers, params=params if url == base_url else None)

                if response.status_code == 200:
                    break
                elif response.status_code == 429:
                    wait_time = retry_delay * (2 ** attempt)
                    self.log.warning(f"Exceeded the rate limit for endpoint {url}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Error fetching data from endpoint {url}: {response.status_code} - {response.text}")

            if response.status_code != 200:
                raise Exception(f"Error fetching data from endpoint {url} after {max_retries} retries: {response.status_code} - {response.text}")

            json_data = response.json()

            # Yield the items from the response
            items = json_data.get("items", json_data.get("data", []))
            if items:
                yield items

            # Check for next page link (because we use pagination)
            links = json_data.get("links", {})
            url = links.get("next")

            # Clear params for subsequent requests (next URL includes params)
            params = {}

    def fetch_order_items(self, order_id, max_retries=5, retry_delay=8):
        """Fetch items for a specific order."""
        items_url = self.build_endpoint_url("order-items", order_id=order_id)
        headers = self.authenticate()

        for attempt in range(max_retries):
            response = self.session.get(items_url, headers=headers)

            if response.status_code == 200:
                break
            elif response.status_code == 403:
                self.log.warning(f"The privileges of the token are insufficient to retrieve order information for this merchant. Access denied for order {order_id}")
                return []
            elif response.status_code == 404:
                # Order might not have items or doesn't exist
                self.log.warning(f"Merchant order was not found. No items found for order {order_id}")
                return []
            elif response.status_code == 429:
                wait_time = retry_delay * (2 ** attempt)
                self.log.warning(f"Exceeded the rate limit for order, while processing {order_id}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                self.log.error(f"Error fetching items for order {order_id}: {response.status_code} - {response.text}")
                return []

        if response.status_code != 200:
            self.log.error(f"Error fetching items for order {order_id} after {max_retries} retries: {response.status_code} - {response.text}")
            return []

        json_data = response.json()
        items = json_data.get("items", json_data.get("data", []))

        # Add order_id to each item for reference
        for item in items:
            item["order_id"] = order_id

        return items

    def fetch_order_item_lines(self, order_id, order_item_id, max_retries=6, retry_delay=12):
        """Fetch lines for a specific order item."""
        lines_url = self.build_endpoint_url(
            "order-item-lines", order_id=order_id, order_item_id=order_item_id
        )
        headers = self.authenticate()

        for attempt in range(max_retries):
            response = self.session.get(lines_url, headers=headers)

            if response.status_code == 200:
                break
            elif response.status_code == 403:
                self.log.warning(f"The privileges of the token are insufficient to retrieve order information for this merchant. Access denied for order {order_id}, item {order_item_id}")
                return []
            elif response.status_code == 404:
                # Order item might not have lines or doesn't exist
                self.log.warning(f"Mechant order, item or line was not found. No lines found for order {order_id}, item {order_item_id}")
                return []
            elif response.status_code == 429:
                wait_time = retry_delay * (2 ** attempt)
                self.log.warning(f"Exceeded the rate limit for order, while processing {order_id}, item {order_item_id}. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                self.log.error(f"Error fetching lines for order {order_id}, item {order_item_id}: {response.status_code} - {response.text}")
                return []

        if response.status_code != 200:
            self.log.error(f"Error fetching lines for order {order_id}, item {order_item_id} after {max_retries} retries: {response.status_code} - {response.text}")
            return []

        json_data = response.json()
        lines = json_data.get("items", json_data.get("data", json_data.get("lines", [])))

        # Add order_id and order_item_id to each line for reference
        for line in lines:
            line["order_id"] = order_id
            line["order_item_id"] = order_item_id

        return lines

    def fetch_item_quantity_snapshot(self, max_retries=4, retry_delay=4):
        """Fetch the warehouse inventory snapshot (single snapshot endpoint).

        Returns the raw response JSON.
        """
        headers = self.authenticate()
        snapshot_url = self.build_endpoint_url("item-quantity-snapshots")

        self.log.info(f"Fetching item quantity snapshot from: {snapshot_url}")

        for attempt in range(max_retries):
            response = self.session.get(snapshot_url, headers=headers)

            if response.status_code == 200:
                break
            elif response.status_code == 429:
                wait_time = retry_delay * (2 ** attempt)
                self.log.warning(f"Exceeded the rate limit for item quantity snapshot. Waiting {wait_time}s before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
            elif response.status_code == 404:
                # 404 could mean: wrong merchant ID, endpoint not available for this account, or wrong path
                error_msg = f"Item quantity snapshot endpoint not found (404). "
                error_msg += f"URL: {snapshot_url}, "
                error_msg += f"Response: {response.text}. "
                error_msg += f"Please verify: 1) Merchant ID is correct, 2) Your account has access to this endpoint, "
                error_msg += f"3) Check Zalando API documentation for correct endpoint path."
                raise Exception(error_msg)
            else:
                raise Exception(f"Error fetching item quantity snapshot: {response.status_code} - {response.text}")

        if response.status_code != 200:
            raise Exception(f"Error fetching item quantity snapshot after {max_retries} retries: {response.status_code} - {response.text}")

        return response.json()
