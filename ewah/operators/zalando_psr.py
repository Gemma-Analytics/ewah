from ewah.constants import EWAHConstants as EC
from ewah.hooks.zalando_psr import EWAHZalandoPSRHook
from ewah.operators.base import EWAHBaseOperator

import json
import time


class EWAHZalandoPSROperator(EWAHBaseOperator):
    """
    # CHANGE description if calling data from another service
    Operator to extract data from the Zalando PSR (Product Status Reports) GraphQL API.
    Uses OAuth 2.0 client-credentials flow for authentication (same as zDirect REST;
    see hook).

    The PSR API is a single GraphQL endpoint (POST .../graphql) that returns a
    near-real-time snapshot of a merchant's products and their statuses. Unlike the
    zDirect REST orders API there is no "updated_after" filter, so we load it as a
    FULL_REFRESH each run.

    Product hierarchy (Zalando attributes live at one of three levels):
      product_model  -> product_config -> product_simple
    The EAN lives at the *simple* level. We flatten to one row per simple so the
    table is a direct EAN <-> SKU (merchant identifier) mapping.

    Connection fields (source_conn):
    - login:    client_id
    - password: client_secret
    - host:     merchant_id

    The API base URL is passed as an operator kwarg (defaults to production). The
    GraphQL endpoint is `{base_url}/graphql`; the token endpoint is `{base_url}/auth/token`.
    """
    # CHANGE if calling data from another service
    _NAMES = ["zalando_psr", "psr"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: False,
    }

    _CONN_TYPE = EWAHZalandoPSRHook.conn_type

    ACCEPTED_QUERIES = (
        "product_models",
    )

    def __init__(
        self,
        query="product_models",
        page_size=100,
        merchant_ids=None,  # Defaults to [conn.host] at execute time if not given.
        status_clusters=None,  # e.g. ["LIVE", "BLOCKED"]; None = no status filter (all).
        # brand_codes/country_codes/season_codes are REQUIRED by the API (non-null
        # arrays). None is sent as an empty array = "no filter on this dimension".
        brand_codes=None,
        country_codes=None,
        season_codes=None,
        base_url="https://api.merchants.zalando.com",
        batch_size=None,  # Swallowed so it doesn't reach EWAHBaseOperator (which rejects it).
        *args,
        **kwargs,
    ):
        assert query in self.ACCEPTED_QUERIES, (
            f"Invalid query '{query}'! Accepted: {', '.join(self.ACCEPTED_QUERIES)}"
        )
        self.query = query
        self.page_size = page_size
        self._merchant_ids = merchant_ids
        self.status_clusters = status_clusters
        self.brand_codes = brand_codes
        self.country_codes = country_codes
        self.season_codes = season_codes
        self.base_url = base_url.rstrip("/")

        super().__init__(*args, **kwargs)

    @property
    def merchant_ids(self):
        """Merchant IDs to filter on. Defaults to [conn.host] when not configured."""
        return self._merchant_ids or [self.source_hook.merchant_id]

    @staticmethod
    def _gql_list(values):
        """Render a Python list of strings as a GraphQL string-array literal."""
        return "[" + ", ".join(json.dumps(v) for v in values) + "]"

    @staticmethod
    def _gql_enum_list(values):
        """Render a Python list as a GraphQL *enum*-array literal (unquoted)."""
        return "[" + ", ".join(str(v) for v in values) + "]"

    def _build_query(self, cursor=None):
        """
        Build the product_models query string for one page.

        The `input` block is rendered inline (rather than via GraphQL variables),
        matching Zalando's own doc examples. `brand_codes`, `country_codes` and
        `season_codes` are required non-null arrays in `ModelsInputType`, so they
        are ALWAYS sent (empty array = no filter on that dimension).

        Field names are confirmed against the live schema via introspection
        (ProductModel / Config / Simple). The merchant SKU lives at three levels:
          - ProductModel.merchant_product_model_id
          - Config.merchant_product_config_id
          - Simple.merchant_product_simple_id  (paired with Simple.ean)
        """
        input_parts = [
            f"merchant_ids: {self._gql_list(self.merchant_ids)}",
            # Required non-null arrays — always present, empty = unfiltered.
            f"brand_codes: {self._gql_list(self.brand_codes or [])}",
            f"country_codes: {self._gql_list(self.country_codes or [])}",
            f"season_codes: {self._gql_list(self.season_codes or [])}",
            f"limit: {int(self.page_size)}",
        ]
        if self.status_clusters:
            # status_clusters is an enum array (e.g. LIVE), so values are unquoted.
            input_parts.append(
                f"status_clusters: {self._gql_enum_list(self.status_clusters)}"
            )
        if cursor:
            input_parts.append(f"cursor: {json.dumps(cursor)}")
        input_block = "{ " + ", ".join(input_parts) + " }"

        # ADJUST the query to match the targeted service
        return f"""
        {{
          psr {{
            product_models(input: {input_block}) {{
              cursor
              items {{
                merchant_product_model_id
                zalando_product_model_id
                name
                brand_name
                product_configs {{
                  merchant_product_config_id
                  zalando_product_config_id
                  season_code
                  product_simples {{
                    ean
                    merchant_product_simple_id
                    zalando_product_simple_id
                    size_codes {{ size length }}
                    status {{ status_detail_code status_cluster }}
                  }}
                }}
              }}
            }}
          }}
        }}
        """

    @staticmethod
    def _first_status(status):
        """Simple.status may come back as a single object or a list of statuses.
        Return a single status dict (the first, if a list) for the flat mapping."""
        if isinstance(status, list):
            return status[0] if status else {}
        return status or {}

    def _flatten(self, items):
        """
        Flatten product_models -> configs -> simples into one row per simple.
        Model- and config-level identifiers are denormalized onto every simple row
        so the table reads as a flat EAN <-> SKU mapping.
        """
        rows = []
        for model in items or []:
            for config in model.get("product_configs", []) or []:
                for simple in config.get("product_simples", []) or []:
                    status = self._first_status(simple.get("status"))
                    size_codes = simple.get("size_codes") or {}
                    rows.append(
                        {
                            # EAN <-> merchant SKU (simple level)
                            "ean": simple.get("ean"),
                            "merchant_product_simple_id": simple.get(
                                "merchant_product_simple_id"
                            ),
                            "zalando_product_simple_id": simple.get(
                                "zalando_product_simple_id"
                            ),
                            "size": size_codes.get("size"),
                            "length": size_codes.get("length"),
                            # config level
                            "merchant_product_config_id": config.get(
                                "merchant_product_config_id"
                            ),
                            "zalando_product_config_id": config.get(
                                "zalando_product_config_id"
                            ),
                            "season_code": config.get("season_code"),
                            # model level
                            "merchant_product_model_id": model.get(
                                "merchant_product_model_id"
                            ),
                            "zalando_product_model_id": model.get(
                                "zalando_product_model_id"
                            ),
                            "model_name": model.get("name"),
                            "brand_name": model.get("brand_name"),
                            # status
                            "status_detail_code": status.get("status_detail_code"),
                            "status_cluster": status.get("status_cluster"),
                        }
                    )
        return rows

    # --------------------------------------------------------------- execute

    def ewah_execute(self, context):
        # Point the hook at the configured base URL (production by default,
        # sandbox if overridden in the DAG's general_config)
        self.source_hook.base_url = self.base_url

        self.log.info(
            f"Starting PSR extraction (query={self.query}, "
            f"merchant_ids={self.merchant_ids}, page_size={self.page_size})"
        )

        cursor = None
        page = 0
        total_rows = 0

        while True:
            query = self._build_query(cursor=cursor)
            data = self.source_hook.execute_graphql(query)

            result = (data.get("psr") or {}).get("product_models") or {}
            items = result.get("items", [])
            rows = self._flatten(items)

            if rows:
                self.upload_data(rows)
                total_rows += len(rows)

            page += 1
            self.log.info(
                f"Page {page}: {len(items)} models -> {len(rows)} simples "
                f"(running total {total_rows})"
            )

            cursor = result.get("cursor")
            if not cursor:
                # Per docs, a null cursor marks the last page.
                break

            # DOUBLE CHECK limits if calling data from another service
            # Stay comfortably under the 240 calls/minute PSR rate limit.
            time.sleep(0.3)

        self.log.info(f"Done. Uploaded {total_rows} product_simple rows.")
