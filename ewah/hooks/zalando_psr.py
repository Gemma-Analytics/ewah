"""Zalando Product Status Reports (PSR) API Connector — GraphQL

API docs: https://developers.merchants.zalando.com/docs/psr-api-overview.html

The PSR API is a single GraphQL endpoint (POST .../graphql). Auth is identical
to the zDirect REST API: OAuth 2.0 client-credentials against
{base_url}/auth/token. The same Bearer token is used for the GraphQL endpoint.
"""

from ewah.hooks.base import EWAHBaseHook

import base64
import json
import time
import requests


class EWAHZalandoPSRHook(EWAHBaseHook):
    """
    Hook to communicate with the Zalando PSR (Product Status Reports) GraphQL API.
    Uses OAuth 2.0 client-credentials flow for authentication (same as zDirect REST).

    Connection fields:
    - login:    client_id
    - password: client_secret
    - host:     merchant_id

    The API base URL defaults to production. The operator overrides it via its
    `base_url` kwarg. The GraphQL endpoint is `{base_url}/graphql`; the token
    endpoint is `{base_url}/auth/token`.
    """

    _ATTR_RELABEL = {
        "client_id": "login",
        "client_secret": "password",
        "merchant_id": "host",
    }

    conn_name_attr = "ewah_zalando_psr_conn_id"
    default_conn_name = "ewah_zalando_psr_default"
    conn_type = "ewah_zalando_psr"
    hook_name = "EWAH Zalando PSR Connection"

    # Defaults; the operator overrides base_url and token state is per instance
    base_url = "https://api.merchants.zalando.com"
    access_token = None
    token_expires_at = 0

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
        # Reuse a single TCP+TLS connection across all paginated requests.
        if not hasattr(self, "_session"):
            self._session = requests.Session()
        return self._session

    def close(self):
        # Called by EWAHBaseOperator at the end of execute()
        if hasattr(self, "_session"):
            self._session.close()
            del self._session

    @property
    def graphql_url(self):
        return f"{self.base_url}/graphql"

    @property
    def merchant_id(self):
        """Merchant ID from the connection host field."""
        return self.conn.merchant_id

    # ------------------------------------------------------------------ auth

    def authenticate(self):
        """
        OAuth 2.0 client-credentials flow. Returns request headers with a Bearer token.
        Token is cached until ~60s before expiry. Mirrors the zDirect REST connector.
        """
        time_now = time.time()

        if self.access_token and time_now < (self.token_expires_at - 60):
            return {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

        client_id = self.conn.client_id
        client_secret = self.conn.client_secret
        encoded_credentials = base64.b64encode(
            f"{client_id}:{client_secret}".encode()
        ).decode()

        token_url = f"{self.base_url}/auth/token"
        token_headers = {
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        token_data = {
            "grant_type": "client_credentials",
            "scope": "access_token_only",
        }

        self.log.info("Requesting OAuth token from Zalando zDirect Auth API...")

        max_retries = 5
        retry_delay = 12
        response = None
        for attempt in range(max_retries):
            response = self.session.post(
                token_url, headers=token_headers, data=token_data
            )
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                wait_time = retry_delay * (2 ** attempt)
                self.log.warning(
                    f"Too many requests obtaining token. Waiting {wait_time}s "
                    f"before retry {attempt + 1}/{max_retries}..."
                )
                time.sleep(wait_time)
            else:
                raise Exception(
                    f"Failed to obtain access token: "
                    f"{response.status_code} - {response.text}"
                )

        if response is None or response.status_code != 200:
            raise Exception(
                f"Failed to obtain access token after {max_retries} retries: "
                f"{getattr(response, 'status_code', 'n/a')} - "
                f"{getattr(response, 'text', 'n/a')}"
            )

        token_response = response.json()
        self.access_token = token_response.get("access_token")
        expires_in = token_response.get("expires_in", 3600)
        self.token_expires_at = time_now + expires_in
        self.log.info(f"Successfully obtained OAuth token (expires in {expires_in}s)")

        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # --------------------------------------------------------------- graphql

    def execute_graphql(self, query, max_retries=6, retry_delay=4):
        """
        POST a GraphQL query string to the PSR endpoint with retry + 429 backoff.
        Returns the `data` object from the response. Raises on GraphQL `errors`.
        """
        payload = {"query": query}

        response = None
        for attempt in range(max_retries):
            headers = self.authenticate()
            response = self.session.post(
                self.graphql_url, headers=headers, data=json.dumps(payload)
            )

            if response.status_code == 200:
                break
            elif response.status_code == 429:
                # PSR rate limit is 240 calls/minute. Back off exponentially on 429.
                wait_time = retry_delay * (2 ** attempt)
                self.log.warning(
                    f"PSR rate limit hit. Waiting {wait_time}s before retry "
                    f"{attempt + 1}/{max_retries}..."
                )
                time.sleep(wait_time)
            else:
                raise Exception(
                    f"GraphQL request failed: {response.status_code} - {response.text}"
                )

        if response is None or response.status_code != 200:
            raise Exception(
                f"GraphQL request failed after {max_retries} retries: "
                f"{getattr(response, 'status_code', 'n/a')} - "
                f"{getattr(response, 'text', 'n/a')}"
            )

        body = response.json()
        if body.get("errors"):
            raise Exception(f"GraphQL errors: {body['errors']}")

        return body.get("data") or {}

    def introspect_type(self, type_name):
        """
        One-off helper to dump the fields of a GraphQL type against the live schema.
        Run this once (e.g. from an Airflow task or a quick local script with a valid
        connection) to confirm the real field names for ProductModel / ProductConfig /
        ProductSimple, then finalize the selection set in the operator's `_build_query`.

        Example: hook.introspect_type("ProductSimple")
        """
        query = f"""
        {{
          __type(name: "{type_name}") {{
            name
            fields {{ name type {{ name kind ofType {{ name kind }} }} }}
          }}
        }}
        """
        data = self.execute_graphql(query)
        type_info = data.get("__type")
        if not type_info:
            self.log.warning(f"Type '{type_name}' not found via introspection.")
            return []
        field_names = [f["name"] for f in type_info.get("fields", [])]
        self.log.info(f"Fields on {type_name}: {field_names}")
        return type_info.get("fields", [])
