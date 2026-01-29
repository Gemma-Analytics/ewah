# EWAH Framework Architecture

## Overview

EWAH (ELT With Airflow Helper) is an Apache Airflow-based ELT framework that extracts data from various sources and loads it into relational data warehouses (PostgreSQL, Snowflake, BigQuery). This document provides a comprehensive technical reference for understanding the source connector logic implemented in this repository.

### Core Philosophy

- **ELT-focused**: Raw data extraction and loading only; no transformations beyond what's needed for relational format
- **Airflow-orchestrated**: DAGs manage scheduling, backfill, and incremental loading
- **Schema isolation**: Each source loads into its own schema (e.g., `raw_salesforce`)
- **Atomic operations**: Data loads into a temporary schema (`_next` suffix) then replaces production data

---

## Architecture

### Component Hierarchy

```
DAG Factory (dags.yml / Python)
    └── Operator (extraction + orchestration logic)
            └── Hook (connection + API communication)
                    └── Uploader (destination handling - OUT OF SCOPE)
```

### Key Directories

| Path | Purpose |
|------|---------|
| `ewah/operators/` | Source-specific extraction logic + parameter handling |
| `ewah/hooks/` | Connection management + API communication |
| `ewah/dag_factories/` | DAG generation patterns (atomic, idempotent, mixed) |
| `ewah/constants/` | Shared constants (DWH engines, strategies) |
| `ewah/utils/yml_loader.py` | YAML config parser with Jinja2 support |
| `airflow/dags/dags.yml` | Example DAG configurations |

---

## How to Analyze a Source Connector

For any connector, follow this process to understand its extraction logic:

### Step 1: Identify the Files

Each connector has up to two files:
- **Operator**: `ewah/operators/<source>.py` - Contains extraction orchestration
- **Hook**: `ewah/hooks/<source>.py` - Contains API/connection logic

### Step 2: Understand the Operator

Look for these key elements in the operator file:

```python
class EWAHExampleOperator(EWAHBaseOperator):
    # 1. YAML aliases - how this connector is referenced in config
    _NAMES = ["example", "ex"]
    
    # 2. Supported extraction strategies
    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }
    
    # 3. __init__ - all source-specific parameters
    def __init__(self, api_resource=None, custom_filter=None, *args, **kwargs):
        # Parameters from YAML config come here
        self.api_resource = api_resource or kwargs["target_table_name"]
        super().__init__(*args, **kwargs)
    
    # 4. ewah_execute - THE CORE EXTRACTION LOGIC
    def ewah_execute(self, context):
        # This is where extraction happens
        # self.data_from and self.data_until are set by parent class
        # self.source_hook is the instantiated hook
        for batch in self.source_hook.get_data(...):
            self.upload_data(batch)
```

### Step 3: Understand the Hook

The hook handles the actual API/database communication:

```python
class EWAHExampleHook(EWAHBaseHook):
    # 1. Connection field mappings
    _ATTR_RELABEL = {"api_key": "password"}  # self.conn.api_key → self.conn.password
    
    # 2. Connection metadata
    conn_type = "ewah_example"
    
    # 3. THE CORE DATA FETCHING METHOD(S)
    def get_data(self, resource, filters, data_from, data_until):
        # Makes API calls
        # Handles pagination
        # Handles rate limiting
        # Returns/yields data as list of dicts
        
    def get_data_in_batches(self, ...):
        # Generator version - yields chunks
        yield batch_of_records
```

### Step 4: Trace the Data Flow

```
1. YAML Config
   ↓
2. DAG Factory creates Operator with config as kwargs
   ↓
3. Operator.__init__() stores source-specific params
   ↓
4. Airflow calls Operator.execute(context)
   ↓
5. Base execute() sets up:
   - self.source_hook (from source_conn_id)
   - self.data_from / self.data_until (from context + config)
   ↓
6. Base execute() calls ewah_execute(context)
   ↓
7. ewah_execute() calls self.source_hook.get_data(...)
   ↓
8. Hook makes API calls, handles pagination, returns data
   ↓
9. ewah_execute() calls self.upload_data(batch) for each batch
```

### Step 5: Find Source-Specific Logic

| What to Find | Where to Look |
|--------------|---------------|
| Required parameters | Operator `__init__()` signature |
| Default values | Operator `__init__()` body |
| API endpoints | Hook class constants or methods |
| Pagination logic | Hook's `get_data()` or `get_data_in_batches()` |
| Authentication | Hook's `__init__()` or property methods |
| Rate limiting | Hook's data fetching methods |
| Data transformation | Hook's `get_cleaner_callables()` or inline in data methods |
| Date filtering | Operator's `ewah_execute()` method |

---

## Complete Connector Walkthrough: Shopify

This section walks through the Shopify connector as a complete example.

### Files Involved

- **Operator**: `ewah/operators/shopify.py`
- **Hook**: `ewah/hooks/shopify.py`

### Operator Analysis (`operators/shopify.py`)

```python
class EWAHShopifyOperator(EWAHBaseOperator):
    _NAMES = ["shopify"]  # Referenced as el_operator: shopify in YAML
    
    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }
```

**Parameters accepted** (from `__init__`):

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `shopify_object` | str | `target_table_name` | API resource (orders, products, etc.) |
| `shop_id` | str | From connection | Shop subdomain |
| `filter_fields` | dict | `{}` | Additional API filters |
| `api_version` | str | `2023-07` | Shopify API version |
| `get_transactions_with_orders` | bool | `False` | Fetch nested transactions |
| `get_events_with_orders` | bool | `False` | Fetch nested events |
| `get_inventory_data_with_product_variants` | bool | `False` | Fetch inventory items |

**Extraction logic** (from `ewah_execute`):

```python
def ewah_execute(self, context):
    # Handle subsequent loading - get max timestamp from existing data
    if self.extract_strategy == EC.ES_SUBSEQUENT and self.test_if_target_table_exists():
        data_from = self.get_max_value_of_column(self.subsequent_field)
        data_until = None
    else:
        data_from = self.data_from    # Set by base class from context
        data_until = self.data_until
    
    # Add metadata (shop_id will be added to each row)
    self._metadata.update({"shop_id": self.shop_id})
    
    # Call hook to fetch data
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
        self.upload_data(batch)  # Send to uploader
```

### Hook Analysis (`hooks/shopify.py`)

**Connection handling**:

```python
class EWAHShopifyHook(EWAHBaseHook):
    conn_type = "ewah_shopify"
    
    # Connection field usage:
    # - login: Shop subdomain (the part before .myshopify.com)
    # - password: Access token
    
    _BASE_URL = "https://{shop}.myshopify.com/admin/api/{version}/{object}.json"
```

**Object configuration** (defines behavior per resource):

```python
_OBJECTS = {
    "orders": {},  # Uses all defaults
    "customers": {},
    "products": {},
    "payouts": {
        "_timestamp_fields": ("date_min", "date_max", "date"),  # Different filter params
        "_datetime_format": "%Y-%m-%d",
        "_object_url": "shopify_payments/payouts",  # Different endpoint
    },
    "balance_transactions": {
        "_is_drop_and_replace": True,  # Forces full refresh only
        "_object_url": "shopify_payments/balance/transactions",
    },
    # ... more objects
}
```

**Core data fetching** (`get_data` method):

```python
def get_data(self, shopify_object, filter_fields, shop_id, version,
             data_from, data_until, add_transactions=False, ...):
    
    # 1. Build URL
    url = self._BASE_URL.format(shop=shop_id, version=version, object=shopify_object)
    
    # 2. Build params with date filters
    params = {"limit": 250}
    if data_from:
        params["updated_at_min"] = self.datetime_to_string(data_from, format)
    if data_until:
        params["updated_at_max"] = self.datetime_to_string(data_until, format)
    
    # 3. Set auth header
    headers = {"X-Shopify-Access-Token": self.conn.password}
    
    # 4. Paginate through results
    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json().get(shopify_object)
        
        # 5. Optionally fetch nested data
        if add_transactions:
            data = self.add_get_transactions(data, shop_id, version, ...)
        
        yield data
        
        # 6. Handle pagination via Link header
        if response.headers.get("Link", "").endswith('rel="next"'):
            url = self.extract_next_url(response.headers["Link"])
            params = {}  # URL contains params
        else:
            break
```

### YAML Configuration Example

```yaml
EL_Shopify:
  dag_strategy: incremental
  el_operator: shopify
  target_schema_name: raw_shopify
  operator_config:
    general_config:
      source_conn_id: shopify_connection
      extract_strategy: subsequent
    tables:
      orders:
        get_transactions_with_orders: true
        get_events_with_orders: true
      products:
        get_inventory_data_with_product_variants: true
      customers: {}
      payouts: {}
```

### Key Extraction Logic Summary

1. **Authentication**: Access token in password field, shop subdomain in login field
2. **Pagination**: Link header with `rel="next"` URL
3. **Date filtering**: `updated_at_min`/`updated_at_max` params (varies by object)
4. **Rate limiting**: None explicit (Shopify handles via 429 responses)
5. **Nested data**: Optional additional API calls per record for transactions/events
6. **Data format**: JSON responses with object name as root key

---

## Connector Architecture

### Pattern Overview

Each connector consists of two files:
1. **Operator** (`ewah/operators/<source>.py`): Handles parameters, extraction strategy, and orchestrates data fetching
2. **Hook** (`ewah/hooks/<source>.py`): Handles authentication, API calls, and data retrieval

### Operator Structure

All operators inherit from `EWAHBaseOperator` and must implement:

```python
class EWAHExampleOperator(EWAHBaseOperator):
    _NAMES = ["example", "ex"]  # YAML aliases for el_operator field
    
    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,    # Load all data every run
        EC.ES_INCREMENTAL: True,     # Load data for time window
        EC.ES_SUBSEQUENT: True,      # Load data newer than last load
    }
    
    _CONN_TYPE = "ewah_example"  # Optional: enforce connection type
    
    def __init__(self, source_specific_param, *args, **kwargs):
        # Set defaults and validate
        kwargs["primary_key"] = kwargs.get("primary_key", "id")
        super().__init__(*args, **kwargs)
        self.source_specific_param = source_specific_param
    
    def ewah_execute(self, context):
        # Main extraction logic
        for batch in self.source_hook.get_data(...):
            self.upload_data(batch)
```

### Hook Structure

All hooks inherit from `EWAHBaseHook`:

```python
class EWAHExampleHook(EWAHBaseHook):
    _ATTR_RELABEL = {"api_key": "password"}  # Map custom attrs to conn fields
    
    conn_name_attr = "ewah_example_conn_id"
    default_conn_name = "ewah_example_default"
    conn_type = "ewah_example"
    hook_name = "EWAH Example Connection"
    
    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["extra", "port"],
            "relabeling": {"password": "API Key"},
        }
    
    @staticmethod
    def get_connection_form_widgets():
        # Optional: custom Airflow UI widgets
        pass
    
    def get_data_in_batches(self, **params):
        # Generator yielding lists of dicts
        yield [{"id": 1, "name": "example"}]
```

---

## Connector Categories

### 1. API-Based Connectors (REST/GraphQL)

**Pattern**: Hook makes HTTP requests, handles pagination and rate limiting.

#### Examples:

| Connector | Files | Key Features |
|-----------|-------|--------------|
| Shopify | `operators/shopify.py`, `hooks/shopify.py` | Pagination via Link header, nested objects (transactions, events) |
| Hubspot | `operators/hubspot.py`, `hooks/hubspot.py` | Associations, properties management, rate limit handling |
| Facebook | `operators/facebook.py`, `hooks/facebook.py` | Async jobs, date range chunking (90 days max) |
| Stripe | `operators/stripe.py`, `hooks/stripe.py` | Resource expansion, multiple object types |
| Google Ads | `operators/google_ads.py`, `hooks/google_ads.py` | GAQL queries, protobuf transformation |

#### Shopify Hook - Key Logic (`hooks/shopify.py`)

```python
# Objects configuration with metadata
_OBJECTS = {
    "orders": {},  # Uses defaults
    "payouts": {
        "_timestamp_fields": ("date_min", "date_max", "date"),
        "_datetime_format": "%Y-%m-%d",
        "_object_url": "shopify_payments/payouts",
    },
    "balance_transactions": {
        "_is_drop_and_replace": True,  # Forces full refresh
        "_object_url": "shopify_payments/balance/transactions",
    },
}

def get_data(self, shopify_object, filter_fields, shop_id, version, 
             data_from, data_until, add_transactions=False, ...):
    # Pagination handling via Link header
    while response.headers.get("Link", "").endswith('rel="next"'):
        url = self.extract_next_url(response.headers["Link"])
        # ... fetch next page
    yield data
```

#### Hubspot Hook - Associations Pattern (`hooks/hubspot.py`)

```python
ACCEPTED_OBJECTS = {
    "companies": ["contacts", "deals", "engagements", "quotes", "tickets"],
    "deals": ["companies", "contacts", "line_items", "quotes"],
    # ...
}

def get_data_in_batches(self, object, properties, associations):
    if associations == "all":
        associations = self.ACCEPTED_OBJECTS.get(object, [])
    
    # Batch fetch associations via POST
    for association in associations:
        request = requests.post(
            assoc_url.format(fromObjectType=object, toObjectType=association),
            data=json.dumps({"inputs": [{"id": str(d["id"])} for d in data]}),
        )
```

### 2. Database Connectors (SQL-Based)

**Pattern**: Base class handles query building; child classes define syntax.

#### Inheritance Chain:
```
EWAHBaseOperator
    └── EWAHSQLBaseOperator (ewah/operators/sql_base.py)
            ├── EWAHPostgresOperator
            ├── EWAHMySQLOperator
            ├── EWAHMSSQLOperator
            ├── EWAHOracleOperator
            └── EWAHBigQueryOperator
```

#### SQL Base Hook (`hooks/sql_base.py`)

```python
class EWAHSQLBaseHook(EWAHBaseHook):
    def get_data_in_batches(self, sql, params, batch_size=10000):
        cursor.execute(sql, params)
        while True:
            data = cursor.fetchmany(batch_size)
            if data:
                yield data
            else:
                break
```

#### PostgreSQL Operator (`operators/sql_postgres.py`)

```python
class EWAHPostgresOperator(EWAHSQLBaseOperator):
    _NAMES = ["pgsql", "postgres", "postgresql"]
    
    _SQL_BASE = 'SELECT\n{columns}\nFROM "{schema}"."{table}"\n'
    _SQL_BASE_SELECT = "WITH raw_data AS (\n\n{select_sql}\n\n) SELECT * FROM raw_data\nWHERE {0}"
    _SQL_COLUMN_QUOTE = '"'
    _SQL_PARAMS = "%({0})s"
```

#### SQL Base Operator Logic (`operators/sql_base.py`)

```python
def ewah_execute(self, context):
    where_clauses = self.where_clauses or []
    
    if self.data_from and self.timestamp_column:
        where_clauses.append(f"{self.timestamp_column} >= %(data_from)s")
        params["data_from"] = self.data_from
    
    if self.subsequent_field and self.test_if_target_table_exists():
        where_clauses.append(f"{self.subsequent_field} > %(previous_max_value)s")
        params["previous_max_value"] = self.get_max_value_of_column(self.subsequent_field)
    
    for batch in self.source_hook.get_data_in_batches(sql, params):
        self.upload_data(batch)
```

### 3. File-Based Connectors

**Pattern**: Read files from storage, parse content, handle compression.

#### S3 Operator (`operators/s3.py`)

```python
_IMPLEMENTED_FORMATS = ["JSON", "JSONL", "AWS_FIREHOSE_JSON", "CSV"]

def __init__(self, bucket_name, file_format, prefix="", suffix=None,
             decompress=False, csv_format_options={}, ...):
    
def ewah_execute(self, context):
    for obj in self._iterate_through_bucket(...):
        if self.file_format == "CSV":
            reader = csv.DictReader(raw_data)
            self.upload_data(list(reader))
        elif self.file_format == "JSONL":
            data = [json.loads(line) for line in file_content.splitlines()]
            self.upload_data(data)
```

### 4. NoSQL Connectors

#### MongoDB (`operators/mongodb.py`, `hooks/mongodb.py`)

```python
# Operator
def ewah_execute(self, context):
    filter_expressions = {"$and": [
        {self.timestamp_field: {"$gte": self.data_from}},
        {self.timestamp_field: {"$lt": self.data_until}},
    ]}
    
    for batch in self.source_hook.get_data_in_batches(
        collection=self.source_collection_name,
        filter_expression=filter_expressions,
    ):
        self.upload_data(batch)

# Hook - cleaner callable for ObjectId conversion
@classmethod
def get_cleaner_callables(cls):
    def clean_data(row):
        for key, value in row.items():
            if isinstance(value, ObjectId):
                row[key] = str(value)
        return row
    return clean_data
```

---

## Extract Strategies

Defined in `ewah/constants/__init__.py`:

| Strategy | Constant | Behavior |
|----------|----------|----------|
| Full Refresh | `EC.ES_FULL_REFRESH` | Load all data every run |
| Incremental | `EC.ES_INCREMENTAL` | Load data within `data_interval_start` to `data_interval_end` |
| Subsequent | `EC.ES_SUBSEQUENT` | Load data newer than the max value of `subsequent_field` |

### Load Strategies

| Strategy | Constant | Behavior |
|----------|----------|----------|
| Upsert | `EC.LS_UPSERT` | Update existing rows by primary key |
| Insert Replace | `EC.LS_INSERT_REPLACE` | Drop and recreate table |
| Insert Add | `EC.LS_INSERT_ADD` | Append rows |

### Default Load Strategy per Extract Strategy

```python
DEFAULT_LS_PER_ES = {
    ES_FULL_REFRESH: LS_INSERT_REPLACE,
    ES_SUBSEQUENT: LS_UPSERT,
    ES_INCREMENTAL: LS_UPSERT,
}
```

---

## DAG Factory Patterns

### 1. Atomic (`dag_factory_atomic.py`)

- Single DAG for full refresh loads
- Schedule: typically daily or hourly
- Use case: Small tables, full refresh only

### 2. Idempotent (`dag_factory_idempotent.py`)

Returns 3 DAGs:
- **Reset DAG**: Pauses other DAGs, deletes metadata and schema
- **Backfill DAG**: Long interval (e.g., 7 days), catches up historical data
- **Current DAG**: Short interval (e.g., 1 hour), keeps data fresh

```python
dags = dag_factory_idempotent(
    dag_name="EL_Source",
    schedule_interval_backfill=timedelta(days=7),
    schedule_interval_future=timedelta(hours=1),
    ...
)
```

### 3. Mixed (`dag_factory_mixed.py`)

Returns 3 DAGs:
- **Atomic DAG**: Periodic full refresh (e.g., daily)
- **Idempotent DAG**: Frequent incremental loads (e.g., hourly)
- **Reset DAG**: Same as idempotent

---

## Configuration via YAML

### Base Configuration

```yaml
base_config:
  dwh_engine: postgres
  dwh_conn_id: dwh
  start_date: 2020-10-01 00:00:00+00:00
  schedule_interval: !!python/object/apply:datetime.timedelta
    - 1  # days
  additional_task_args:
    retries: 1
    retry_delay: !!python/object/apply:datetime.timedelta
      - 0
      - 300  # seconds
```

### DAG Configuration

```yaml
el_dags:
  EL_Shopify:
    dag_strategy: incremental  # or: atomic, fr, mixed, fullcremental
    el_operator: shopify       # Maps to _NAMES in operator class
    target_schema_name: raw_shopify
    operator_config:
      general_config:          # Applied to all tables
        source_conn_id: shopify
        extract_strategy: subsequent
      tables:
        orders:                # Table name = target table name
          primary_key: id
          get_transactions_with_orders: true
        customers: {}          # Empty = use general_config only
        products:
          target_table_name: product_catalog  # Override target name
```

### Jinja2 Templating in YAML

```yaml
tables:
  {% for currency in airflow_variables['currencies'].split(",") %}
    daily_fx_rates_{{ currency|lower() }}:
      currency_pair:
        - USD
        - {{ currency|upper() }}
  {% endfor %}
```

### File Inclusion

```yaml
EL_Secret_Source: !yml_from_file secret_dag_config.yml
extra_sql: !text_from_file custom_query.sql
```

---

## Parameter Flow

### From YAML to Operator

```
dags.yml
    └── el_dags.<dag_name>.operator_config.general_config
            ↓ (merged with)
        el_dags.<dag_name>.operator_config.tables.<table_name>
            ↓ (passed to)
        DAG Factory (adds dwh_engine, target_schema_name, etc.)
            ↓ (creates)
        Operator.__init__(**kwargs)
```

### Inside Operator.__init__

1. **EWAHBaseOperator.base_init()** receives all kwargs:
   - `source_conn_id` → resolved to `self.source_conn` and `self.source_hook`
   - `dwh_engine`, `dwh_conn_id` → DWH configuration
   - `extract_strategy`, `load_strategy` → validated
   - `target_table_name`, `target_schema_name` → destination
   - `primary_key`, `load_data_from`, `load_data_until` → data handling

2. **Child operator** adds source-specific params:
   ```python
   def __init__(self, shopify_object=None, filter_fields=None, ...):
       self.shopify_object = shopify_object or kwargs["target_table_name"]
   ```

### During Execution

```python
def execute(self, context):
    # 1. Resolve source connection
    self.source_conn = EWAHBaseHook.get_connection(self.source_conn_id)
    self.source_hook = self.source_conn.get_hook()
    
    # 2. Calculate date ranges based on extract strategy
    if self.extract_strategy == EC.ES_INCREMENTAL:
        self.data_from = context["data_interval_start"]
        self.data_until = context["data_interval_end"]
    
    # 3. Initialize uploader
    self.uploader = self.uploader_class(...)
    
    # 4. Call child's extraction logic
    self.ewah_execute(context)
    
    # 5. Finalize and commit
    self.uploader.finalize_upload()
    self.uploader.commit()
```

---

## Connection Handling

### Airflow Connection Fields

Standard fields mapped via `_ATTR_RELABEL`:

| Airflow Field | Common Usage |
|---------------|--------------|
| `host` | API endpoint / hostname |
| `login` | Username / API ID |
| `password` | Password / API key / token |
| `schema` | Database name / Account ID |
| `port` | Port number |
| `extra` | JSON with additional fields |

### Custom Widgets

```python
@staticmethod
def get_connection_form_widgets():
    return {
        "extra__ewah_example__access_token": StringField(
            "Access Token", widget=BS3PasswordFieldWidget()
        ),
    }
```

Access in hook: `self.conn.access_token`

### SSH Tunneling

SQL hooks support SSH tunneling via `ssh_conn_id`:

```python
if hasattr(self.conn, "ssh_conn_id") and self.conn.ssh_conn_id:
    self._ssh_hook = EWAHBaseHook.get_hook_from_conn_id(self.conn.ssh_conn_id)
    self.local_bind_address = self._ssh_hook.start_tunnel(host, port)
```

---

## Complete Connector List

### API Connectors

| Operator File | Hook File | YAML Names |
|---------------|-----------|------------|
| `aircall.py` | `aircall.py` | `aircall` |
| `airflow.py` | `airflow.py` | `airflow` |
| `airtable.py` | `airtable.py` | `airtable` |
| `amazon_ads.py` | `amazon_ads.py` | `amazon_ads` |
| `amazon_seller_central.py` | `amazon_seller_central.py` | `amazon_reporting` |
| `braze.py` | `braze.py` | `braze` |
| `facebook.py` | `facebook.py` | `facebook`, `fb` |
| `google_ads.py` | `google_ads.py` | `gads`, `google_ads` |
| `google_analytics.py` | `google_analytics.py` | `ga`, `google_analytics` |
| `google_cloud_storage.py` | `google_cloud_storage.py` | `gcs` |
| `google_sheets.py` | N/A (uses gspread) | `gsheets` |
| `hubspot.py` | `hubspot.py` | `hubspot` |
| `infigo.py` | `infigo.py` | `infigo` |
| `linkedin.py` | `linkedin.py` | `linkedin` |
| `linkedin_ads.py` | `linkedin.py` | `linkedin_ads` |
| `mailchimp.py` | `mailchimp.py` | `mailchimp` |
| `mailingwork.py` | N/A | `mailingwork` |
| `personio.py` | `personio.py` | `personio` |
| `pipedrive.py` | `pipedrive.py` | `pipedrive` |
| `plentymarkets.py` | `plentymarkets.py` | `plentymarkets` |
| `rapidmail.py` | `rapidmail.py` | `rapidmail` |
| `recurly.py` | `recurly.py` | `recurly` |
| `salesforce.py` | `salesforce.py` | `sf`, `salesforce` |
| `sevdesk.py` | `sevdesk.py` | `sevdesk` |
| `sharepoint.py` | `sharepoint.py` | `sharepoint` |
| `shopify.py` | `shopify.py` | `shopify` |
| `shopify_graphql.py` | `shopify_graphql.py` | `shopify_graphql` |
| `stripe.py` | `stripe.py` | `stripe` |
| `zendesk.py` | N/A | `zendesk` |

### Database Connectors

| Operator File | Hook File | YAML Names |
|---------------|-----------|------------|
| `sql_postgres.py` | `postgres.py` | `pgsql`, `postgres`, `postgresql` |
| `sql_mysql.py` | `mysql.py` | `mysql` |
| `sql_mssql.py` | `mssql.py` | `mssql` |
| `sql_oracle.py` | `oracle.py` | `oracle` |
| `sql_bigquery.py` | `bigquery.py` | `bq`, `bigquery` |

### File/NoSQL Connectors

| Operator File | Hook File | YAML Names |
|---------------|-----------|------------|
| `s3.py` | N/A (uses provider) | `s3` |
| `dynamodb.py` | `aws.py` | `dynamodb` |
| `mongodb.py` | `mongodb.py` | `mongo`, `mongodb` |

### Utility Connectors

| Operator File | Hook File | YAML Names |
|---------------|-----------|------------|
| `fx.py` | N/A | `fx` |
| `fx_yfinance.py` | N/A | `fx_yfinance` |
| `google_maps.py` | N/A | `gmaps` |

---

## Extraction Logic Components

For each connector, the extraction logic consists of these distinct components:

### 1. Authentication

**Where to find**: Hook class

| Component | Location | Example |
|-----------|----------|---------|
| Required credentials | `_ATTR_RELABEL` dict | `{"api_key": "password"}` |
| Connection fields | `get_ui_field_behaviour()` | Shows which fields are used |
| Custom fields | `get_connection_form_widgets()` | Extra fields in `self.conn.extra_dejson` |
| Auth header/setup | Hook's data methods or `__init__` | `headers["Authorization"] = f"Bearer {token}"` |

### 2. API/Database Communication

**Where to find**: Hook class data methods

| Component | Typical Method Names | What to Extract |
|-----------|---------------------|-----------------|
| Endpoint construction | Class constants, method params | URL patterns, query building |
| Request execution | `get_data()`, `get_data_in_batches()` | HTTP methods, request params |
| Pagination | Inside data methods | Link headers, cursor params, offset logic |
| Rate limiting | Inside data methods or decorators | Sleep calls, retry logic |
| Error handling | Inside data methods | Status code checks, exceptions |

### 3. Date/Time Filtering

**Where to find**: Operator's `ewah_execute()` + Hook's data methods

| Extract Strategy | Operator Logic | Hook Logic |
|------------------|---------------|------------|
| Full Refresh | No date filtering | May accept optional date params |
| Incremental | Uses `self.data_from`, `self.data_until` from context | Applies to API query |
| Subsequent | Gets max value via `get_max_value_of_column()` | Filters by `> last_value` |

### 4. Data Transformation

**Where to find**: Hook class

| Component | Location | Purpose |
|-----------|----------|---------|
| Type conversion | `get_cleaner_callables()` | e.g., ObjectId → string |
| Field flattening | Data methods | e.g., Protobuf → dict |
| Date parsing | Data methods | String → datetime |
| Nested unpacking | Data methods | e.g., `datum.update(datum.pop("properties"))` |

### 5. Resource Configuration

**Where to find**: Hook class constants + Operator params

| Component | Example Location | Purpose |
|-----------|-----------------|---------|
| Available resources | Hook's `_OBJECTS`, `ACCEPTED_OBJECTS` | What can be fetched |
| Resource metadata | Hook constants | Endpoints, field names, behaviors |
| Resource selection | Operator params | `shopify_object`, `object`, `resource` |

### 6. Nested/Related Data

**Where to find**: Operator params + Hook methods

| Pattern | Example | Where |
|---------|---------|-------|
| Additional API calls | `get_transactions_with_orders` | Operator param triggers hook method |
| Associations | `associations` param in Hubspot | Hook fetches via separate endpoint |
| Expansion | `expand` param in Stripe | Passed to API |

---

## Extraction Logic by Connector Type

### API Connectors - What to Extract

```
From Operator:
├── __init__() parameters → Configuration options
├── ewah_execute() → Date handling, resource selection, batch loop
└── _metadata updates → Additional fields per record

From Hook:
├── Class constants → Base URLs, object configs, defaults
├── get_data() / get_data_in_batches() → Full API logic
│   ├── URL construction
│   ├── Header setup (auth)
│   ├── Parameter building
│   ├── Request execution
│   ├── Response parsing
│   ├── Pagination handling
│   └── Rate limit handling
└── get_cleaner_callables() → Data transformations
```

### SQL Connectors - What to Extract

```
From Operator (sql_base.py + specific):
├── _SQL_BASE → Query template
├── _SQL_PARAMS → Parameter placeholder format
├── ewah_execute() → WHERE clause building
└── timestamp_column handling → Date filtering

From Hook (sql_base.py + specific):
├── _get_db_conn() → Connection creation
├── get_data_in_batches() → Cursor-based fetching
└── SSH tunnel support → Optional tunneling
```

### File Connectors - What to Extract

```
From Operator:
├── File format handling (JSON, CSV, etc.)
├── Compression handling
├── Bucket/path iteration
└── ewah_execute() → File processing loop

From Hook (if exists):
└── Storage client initialization
```

---

## Migration Considerations

### What Each Standalone App Needs

1. **Authentication/Connection Logic**
   - Extract from hook's `get_connection_form_widgets()` and `get_ui_field_behaviour()`
   - Implement connection string building from hook's `__init__` or property methods

2. **API Communication Logic**
   - Extract from hook's data fetching methods (e.g., `get_data()`, `get_data_in_batches()`)
   - Handle pagination, rate limiting, error retries

3. **Data Transformation**
   - Extract `get_cleaner_callables()` from hooks
   - Handle data type conversions (e.g., `ObjectId` to string in MongoDB)

4. **Extraction Strategy Logic**
   - Handle date filtering (`data_from`, `data_until`)
   - Handle subsequent loading (tracking max values)
   - From operator's `ewah_execute()` method

5. **Source-Specific Parameters**
   - From operator's `__init__()` method
   - Document all parameters with defaults and validation rules

### Airflow-Specific Code to Replace

| EWAH Pattern | Migration Approach |
|--------------|-------------------|
| `context["data_interval_start"]` | Pass as CLI argument or env var |
| `self.log.info()` | Standard Python logging |
| `self.upload_data(data)` | Replace with output writer (file, stdout, etc.) |
| `self.get_max_value_of_column()` | External state storage (file, database, API) |
| `self.test_if_target_table_exists()` | Check state storage |
| Airflow connections | Environment variables or config files |
| `self.source_hook` | Direct instantiation of API client |

### Output Format

Each connector should output data in a standardized format:
- JSONL (newline-delimited JSON) for streaming
- Include metadata fields similar to EWAH's `_metadata`:
  ```json
  {
    "_ewah_executed_at": "2024-01-15T10:30:00Z",
    "_ewah_extract_strategy": "incremental",
    "id": 123,
    "name": "Example"
  }
  ```

---

## Quick Reference: Finding Extraction Logic

| What You Need | Primary File | Specific Location |
|---------------|--------------|-------------------|
| Connector name for YAML | Operator | `_NAMES` class attribute |
| Supported strategies | Operator | `_ACCEPTED_EXTRACT_STRATEGIES` |
| Config parameters | Operator | `__init__()` signature |
| Date filtering logic | Operator | `ewah_execute()` method |
| API base URL | Hook | Class constants (`_BASE_URL`, etc.) |
| Authentication | Hook | `__init__`, properties, or data methods |
| Pagination | Hook | Inside `get_data()` / `get_data_in_batches()` |
| Available resources | Hook | `_OBJECTS`, `ACCEPTED_OBJECTS`, or similar |
| Data cleaning | Hook | `get_cleaner_callables()` |
| Nested data fetching | Hook | Helper methods called from main data method |

---

## Testing Reference

Example DAG configurations in `airflow/dags/dags.yml` demonstrate:
- All connector types and their parameters
- Different DAG strategies
- Jinja2 templating patterns
- Override patterns (general_config vs table-specific)

Use these as test cases for migrated connectors.
