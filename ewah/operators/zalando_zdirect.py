from ewah.constants import EWAHConstants as EC
from ewah.hooks.zalando_zdirect import EWAHZalandoZDirectHook
from ewah.operators.base import EWAHBaseOperator

from datetime import datetime
import time


# TO DO: Check a mechanism for orphanted order items in case an order has been deleted from Zalando
class EWAHZalandoZDirectOperator(EWAHBaseOperator):
    """
    Operator to extract data from Zalando zDirect API.
    Uses OAuth 2.0 client credentials flow for authentication (see hook).

    Connection fields:
    - login: client_id
    - password: client_secret
    - host: merchant_id

    The API base URL is passed as an operator kwarg (defaults to production).
    Override `base_url` in the DAG's `general_config` to point at sandbox.
    """

    _NAMES = ["zalando_zdirect", "zalando", "zdirect"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHZalandoZDirectHook.conn_type

    ACCEPTED_ENDPOINTS = (
        "orders",
        "order-items",  # Requires fetching orders first, then items per order
        "order-item-lines",  # Requires: orders → order items → order item lines
        # "customer-returned-items", # TO DO: Investigate if it's currently being used by the client
        "item-quantity-snapshots",  # Warehouse inventory snapshots
    )

    def __init__(
        self,
        endpoint,
        page_size=50,
        lookback_days=365,  # For endpoints requiring date range (e.g., customer-returned-items)
        base_url="https://api.merchants.zalando.com",  # Override to sandbox in the DAG config if needed
        batch_size=None,  # Swallowed here so it doesn't reach EWAHBaseOperator (which rejects it)
        *args,
        **kwargs,
    ):
        assert endpoint in self.ACCEPTED_ENDPOINTS, (
            f"Invalid endpoint '{endpoint}'! Accepted: {', '.join(self.ACCEPTED_ENDPOINTS)}"
        )
        self.endpoint = endpoint
        self.page_size = page_size
        self.lookback_days = lookback_days
        self.base_url = base_url.rstrip("/")

        super().__init__(*args, **kwargs)

    def _dwh_hook(self):
        """Create the configured EWAH DWH hook for neccessary lookup queries."""
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            from ewah.hooks.snowflake import EWAHSnowflakeHook
            return EWAHSnowflakeHook(conn_id=self.dwh_conn_id)
        if self.dwh_engine == EC.DWH_ENGINE_POSTGRES:
            from ewah.hooks.postgres import EWAHPostgresHook
            return EWAHPostgresHook(conn_id=self.dwh_conn_id)
        raise RuntimeError(f"(!)Unsupported DWH Engine: {self.dwh_engine}")

    def _qualified_target_table(self, table_name):
        """Build a fully-qualified identifier for the table in the _NEXT schema."""
        target_schema = getattr(self, "target_schema_name", None) or getattr(self, "schema_name", None)
        if not target_schema:
            raise RuntimeError("Cannot determine target schema name")

        # target_schema_name may include the database (e.g. "DEV_MICHAL_RAW.ZALANDO_ZDIRECT")
        database = None
        if "." in target_schema:
            database, target_schema = target_schema.split(".", 1)

        schema = target_schema + (getattr(self, "target_schema_suffix", None) or "")

        if database:
            return f'"{database}"."{schema}"."{table_name}"'
        return f'"{schema}"."{table_name}"'

    def _incremental_cutoff(self, context):
        """Return the cutoff datetime for incremental loading, or None for a
        full load (no API filter).

        Returns None when the target table doesn't exist or is empty — i.e.
        on a true first run, or after the schema has been wiped. The caller
        then issues an unfiltered API request and reloads from scratch.

        When prior data exists, returns the earlier of:
        - MAX(self.subsequent_field) from the target table
        - context["data_interval_start"] (previous DAG run's start)

        Picking the earlier of the two protects against:
        - Intra-task drift: `_ewah_executed_at` is stamped per upload batch,
          so MAX advances during the task itself. An update arriving mid-task
          gets a timestamp older than the eventual MAX and would be skipped
          next run if MAX alone were the cutoff.
        - Missed DAG runs (catchup=False): `data_interval_start` only rewinds
          one schedule interval; the DB MAX is the safety net for skipped runs.
        """
        if not self.test_if_target_table_exists():
            return None

        last_loaded = self.get_max_value_of_column(self.subsequent_field)
        if last_loaded is None:
            return None

        last_loaded = self._coerce_to_datetime(last_loaded)
        dag_start = context.get("data_interval_start")
        if dag_start is None:
            return last_loaded

        return min(last_loaded, self._coerce_to_datetime(dag_start))

    @staticmethod
    def _coerce_to_datetime(value):
        """Return a tz-aware datetime regardless of whether the input is a string
        (e.g. ITEM_QUANTITY_SNAPSHOTS.snapshot_created stored as VARCHAR), a
        naive datetime, or already a tz-aware datetime/Pendulum DateTime.

        Python 3.10's `datetime.fromisoformat` doesn't accept the trailing 'Z',
        so we normalize it to '+00:00' first.
        """
        from datetime import timezone
        if isinstance(value, str):
            s = value.strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            value = datetime.fromisoformat(s)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value

    @staticmethod
    def _to_zalando_timestamp(value):
        """Format a datetime as ISO-8601 with second precision and an explicit
        UTC offset, e.g. '2026-05-15T19:58:38+00:00'.

        Matches Zalando's documented format (their example: '2017-01-01T09:11:48+01:00').
        Both fractional seconds and the 'Z' shorthand cause the API to silently
        drop the filter, returning the unfiltered result set.
        """
        if hasattr(value, "replace") and hasattr(value, "isoformat"):
            # tz-aware datetime: strip microseconds, isoformat emits '+HH:MM' offset
            s = value.replace(microsecond=0).isoformat()
        else:
            s = str(value)
            # Strip fractional seconds if present in string form
            import re
            s = re.sub(r"\.\d+", "", s)
        # Normalize 'Z' to explicit '+00:00'
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return s

    def get_order_ids_from_database(self):
        """Return distinct order ids from the ORDERS _NEXT table, so only these are passed for further import job."""
        try:
            query = f'SELECT DISTINCT "id" FROM {self._qualified_target_table("ORDERS")}'
            self.log.info(f"Executing query: {query}")
            hook = self._dwh_hook()
            try:
                records = hook.execute_and_return_result(query)
            finally:
                hook.close()
            order_ids = [row[0] for row in records if row and row[0]]
            self.log.info(f"Found {len(order_ids)} order IDs in the database")
            return order_ids
        except Exception as e:
            self.log.info(f"ORDERS table may not exist yet or query failed: {e}. Returning empty list.")
            return []

    def get_order_item_pairs_from_database(self, since=None):
        """Return distinct (order_id, id) pairs from the ORDER_ITEMS _NEXT table.

        If `since` is provided, restrict to rows whose `_ewah_executed_at` is >= since.
        `since` may be a datetime or an ISO-8601 string and is bound as a query
        parameter.
        """
        try:
            table = self._qualified_target_table("ORDER_ITEMS")
            query = f'SELECT DISTINCT "order_id", "id" FROM {table}'
            params = None
            if since is not None:
                since_str = since.isoformat() if hasattr(since, "isoformat") else str(since)
                query += ' WHERE "_ewah_executed_at" >= %(since)s'
                params = {"since": since_str}
            self.log.info(f"Executing query: {query}")
            hook = self._dwh_hook()
            try:
                records = hook.execute_and_return_result(query, params=params)
            finally:
                hook.close()
            pairs = [(row[0], row[1]) for row in records if row and row[0] and row[1]]
            self.log.info(f"Found {len(pairs)} distinct order item pairs in database")
            return pairs
        except Exception as e:
            self.log.info(f"ORDER_ITEMS table may not exist yet or query failed: {e}. Returning empty list.")
            return []

    def fetch_all_order_ids(self, params=None, from_database=False):
        """
        Fetch order IDs either from database or from API.

        Args:
            params: API query parameters
            from_database: If True, get IDs from database; if False, get from API
        """
        if from_database:
            order_ids = self.get_order_ids_from_database()
            if order_ids:
                return order_ids
            else:
                self.log.info("No order IDs found in database. Fetching from API instead.")

        # Fetch from API
        orders_url = self.source_hook.build_endpoint_url("orders")
        order_ids = []

        self.log.info("Fetching all order IDs from API...")
        for orders_chunk in self.source_hook.fetch_paginated_data(
            orders_url, params, page_size=self.page_size
        ):
            for order in orders_chunk:
                # Extract order_id - adjust field name based on actual API response
                order_id = order.get("order_number") or order.get("id") or order.get("order_id")
                if order_id:
                    order_ids.append(order_id)

        self.log.info(f"Found {len(order_ids)} orders from API")
        return order_ids

    def ewah_execute(self, context):
        """Main execution method called by EWAH framework."""
        # Point the hook at the configured base URL (production by default,
        # sandbox if overridden in the DAG's general_config)
        self.source_hook.base_url = self.base_url

        endpoint_url = self.source_hook.build_endpoint_url(self.endpoint)
        params = {}

        # customer-returned-items endpoint requires date_between parameter (format: YYYY-MM-DD/YYYY-MM-DD)
        # if self.endpoint == "customer-returned-items":
        #    end_date = datetime.now().strftime("%Y-%m-%d")
        #    start_date = (datetime.now() - timedelta(days=self.lookback_days)).strftime("%Y-%m-%d")
        #    params["date_between"] = f"{start_date}/{end_date}"
        #    self.log.info(f"Using date range: {start_date} to {end_date} (lookback_days={self.lookback_days})")

        # Handle subsequent/incremental loading
        if self.extract_strategy == EC.ES_SUBSEQUENT:
            cutoff = self._incremental_cutoff(context)

            if cutoff is None:
                self.log.info(f"First run for {self.endpoint}. No filter applied (full backfill).")
            else:
                cutoff_str = self._to_zalando_timestamp(cutoff)
                self.log.info(f"Fetching data from {cutoff_str} onwards.")

                if self.endpoint == "orders":
                    params["last_updated_after"] = cutoff_str
                    self.log.info(f"Using filter: last_updated_after={cutoff_str}")
                elif self.endpoint == "customer-returned-items":
                    # date_between uses date-only granularity
                    end_date = datetime.now().strftime("%Y-%m-%d")
                    start_date = cutoff_str[:10]
                    params["date_between"] = f"{start_date}/{end_date}"

        self.log.info(f"Starting extraction from endpoint: {self.endpoint}")

        # Special handling for item-quantity-snapshots: single snapshot endpoint
        if self.endpoint == "item-quantity-snapshots":
            json_data = self.source_hook.fetch_item_quantity_snapshot()
            snapshot = json_data.get("item_quantity_snapshot", {})
            item_quantities = snapshot.get("item_quantities", [])
            created = json_data.get("created")

            # Add created timestamp to each item quantity for reference and incremental loading
            for item in item_quantities:
                item["snapshot_created"] = created

            if item_quantities:
                self.upload_data(item_quantities)
                self.log.info(f"Uploaded {len(item_quantities)} item quantities from snapshot created at {created}")
            else:
                self.log.info("No item quantities found in snapshot")

        # Special handling for order-items due to performance issues: need to fetch orders first, then items per order
        elif self.endpoint == "order-items":
            # Only SUBSEQUENT runs use an incremental cutoff; FULL_REFRESH (if ever
            # configured in table_configs) leaves cutoff=None and falls through to
            # the full-load branch below.
            cutoff = None
            if self.extract_strategy == EC.ES_SUBSEQUENT:
                cutoff = self._incremental_cutoff(context)

            if cutoff is not None:
                cutoff_str = self._to_zalando_timestamp(cutoff)
                self.log.info(f"Fetching ORDER_ITEMS for orders updated after: {cutoff_str}")
                params["last_updated_after"] = cutoff_str
                order_ids = self.fetch_all_order_ids(params, from_database=False)
            else:
                # First run / full load: fetch items for all orders in database
                self.log.info("Full load: Fetching order IDs from database...")
                order_ids = self.fetch_all_order_ids(params, from_database=True)

            if not order_ids:
                self.log.info("No order IDs found. Skipping ORDER_ITEMS extraction.")
                return

            all_items = []
            batch_size = 10000  # Upload in batches

            self.log.info(f"Fetching items for {len(order_ids)} orders")

            for i, order_id in enumerate(order_ids):
                items = self.source_hook.fetch_order_items(order_id)
                all_items.extend(items)

                # Log progress every 1000 orders
                if (i + 1) % 1000 == 0:
                    self.log.info(f"Processed {i + 1}/{len(order_ids)} orders, collected {len(all_items)} items")

                # Upload in batches to avoid memory issues
                if len(all_items) >= batch_size:
                    self.upload_data(all_items)
                    all_items = []

                # Small delay to avoid rate limiting (lower than 0.5s causes rate limit handling as per Jan 2026)
                time.sleep(0.5)

            # Upload remaining items
            if all_items:
                self.upload_data(all_items)

            self.log.info(f"Done extracting order items. Collected {len(all_items)} items")

        # Special handling for order-item-lines: orders → order items → order item lines.
        # The DAG enforces ORDERS → ORDER_ITEMS → ORDER_ITEM_LINES, so by the time we
        # run, ORDER_ITEMS has been freshly populated in the _NEXT schema. We derive
        # (order_id, item_id) pairs directly from that table instead of re-fetching
        # them per-order from the API — which avoids N redundant calls per run and the
        # 0.5s sleep that comes with them.
        elif self.endpoint == "order-item-lines":
            cutoff = None
            if self.extract_strategy == EC.ES_SUBSEQUENT:
                cutoff = self._incremental_cutoff(context)

            if cutoff is not None:
                self.log.info(f"Incremental: fetching ORDER_ITEMS rows loaded since {cutoff}")
            else:
                self.log.info("Full load: fetching all ORDER_ITEMS pairs from database")

            order_item_pairs = self.get_order_item_pairs_from_database(since=cutoff)

            if not order_item_pairs:
                self.log.info("No order items to process. Skipping ORDER_ITEM_LINES extraction.")
                return

            all_lines = []
            batch_size = 10000
            total_items = len(order_item_pairs)
            self.log.info(f"Processing {total_items} order items for ORDER_ITEM_LINES extraction")

            for item_idx, (order_id, order_item_id) in enumerate(order_item_pairs):
                lines = self.source_hook.fetch_order_item_lines(order_id, order_item_id)
                all_lines.extend(lines)

                # Small delay to avoid rate limiting; 429 backoff in fetch_order_item_lines is the safety net
                time.sleep(0.05)

                if (item_idx + 1) % 1000 == 0:
                    self.log.info(f"Processed {item_idx + 1}/{total_items} items, collected {len(all_lines)} lines")

                if len(all_lines) >= batch_size:
                    self.upload_data(all_lines)
                    all_lines = []

            if all_lines:
                self.upload_data(all_lines)

            self.log.info(f"Done extracting order item lines from {total_items} order items")
        else:
            # Standard endpoint handling
            for data_chunk in self.source_hook.fetch_paginated_data(
                endpoint_url, params, page_size=self.page_size
            ):
                if data_chunk:
                    self.upload_data(data_chunk)
                else:
                    self.log.info(f"No data returned for endpoint {self.endpoint}")
                    break

            self.log.info("Done extracting Zalando zDirect data")
