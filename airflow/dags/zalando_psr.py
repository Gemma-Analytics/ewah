# Zalando Product Status Reports (PSR) API Connector — GraphQL
# ADJUST the DAG's name and table configurations to match the targeted service
# API docs: https://developers.merchants.zalando.com/docs/psr-api-overview.html

from ewah.constants import EWAHConstants as EC
from ewah.dag_factories.dag_factory_atomic import dag_factory_atomic
from ewah.operators.zalando_psr import EWAHZalandoPSROperator

from datetime import datetime, timedelta

# In the client repo this comes from `_defaults.py`; defined inline here
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "email_on_failure": False,
    "email": [],
    "owner": "EWAH",
}

# ADJUST if calling data from another service
# Table configurations. PSR is a snapshot with no incremental field, so FULL_REFRESH.
# Grain = product_simple, keyed on EAN (the EAN <-> SKU mapping the client needs).
table_configs = {
    "PRODUCT_SIMPLES": {
        "query": "product_models",
        "page_size": 100,
        "extract_strategy": EC.ES_FULL_REFRESH,
        "primary_key": "ean",
    },
}

# CHANGE DAG name if calling data from another service
# Creates the Zalando PSR DAG. Uses a dedicated source connection (`zalando_psr`)
# and loads into a separate ZALANDO_PSR schema, keeping PSR tables apart from the
# zDirect REST tables. Reuses the shared baesiq warehouse connection.
zalando_psr_dag = dag_factory_atomic(
    dag_name="EL_Zalando_PSR",
    dwh_engine=EC.DWH_ENGINE_SNOWFLAKE,
    dwh_conn_id="dwh",
    start_date=datetime(2026, 6, 30),
    el_operator=EWAHZalandoPSROperator,
    operator_config={
        "general_config": {
            "source_conn_id": "zalando_psr",
            "params": {"slack_channel": "xxx"},
            "batch_size": 10000,
        },
        "tables": table_configs,
    },
    # CHANGE target schema name if calling data from another service
    target_schema_name="ZALANDO_PSR",
    target_schema_suffix="_NEXT",
    default_args=default_args,
    schedule_interval="30 4 * * *",
    dagrun_timeout_factor=1,
    additional_dag_args={
        "max_active_tasks": 1,
        "dagrun_timeout": timedelta(hours=4),
    },
)[0]
