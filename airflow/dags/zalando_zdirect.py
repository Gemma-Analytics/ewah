# Zalando zDirect API Connector
# API docs: https://developers.merchants.zalando.com/docs/index.html

from airflow.models import Variable
from ewah.constants import EWAHConstants as EC
from ewah.dag_factories.dag_factory_atomic import dag_factory_atomic
from ewah.operators.zalando_zdirect import EWAHZalandoZDirectOperator

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

# Table configurations
table_configs = {
    "ORDERS": {
        "endpoint": "orders",
        "page_size": 200,
        "extract_strategy": "subsequent",
        "subsequent_field": "_ewah_executed_at",
        "primary_key": "id",
    },
    "ORDER_ITEMS": {
        "endpoint": "order-items",
        "page_size": 400,
        "extract_strategy": "subsequent",
        "subsequent_field": "_ewah_executed_at",  # Own load timestamp; drives last_updated_after on the next run
        "primary_key": "id",
    },
    "ORDER_ITEM_LINES": {
        "endpoint": "order-item-lines",
        "page_size": 200,
        "extract_strategy": "subsequent",
        "subsequent_field": "_ewah_executed_at",  # Own load timestamp; drives last_updated_after on the next run
        "primary_key": "id",
    },
    # "CUSTOMER_RETURNED_ITEMS": {
    #    "endpoint": "customer-returned-items",
    # },
    "ITEM_QUANTITY_SNAPSHOTS": {
        "endpoint": "item-quantity-snapshots",
        "extract_strategy": "subsequent",
        "subsequent_field": "snapshot_created",
        "primary_key": "ean",
    },
}

# Read DAG-run timeout from an Airflow Variable. Logged here at DAG-parse time so
# you can confirm in the scheduler logs which value is currently in effect
# (e.g. while bumping it for a backfill).
_dagrun_timeout_hours = int(
    Variable.get("zalando_dagrun_timeout_hours", default_var=4)
)
print(
    f"[EL_Zalando_zDirect] dagrun_timeout = {_dagrun_timeout_hours}h "
    f"(Airflow Variable 'zalando_dagrun_timeout_hours', default=4)"
)

# Creates the Zalando zDirect DAG
zalando_dag = dag_factory_atomic(
    dag_name="EL_Zalando_zDirect",
    dwh_engine=EC.DWH_ENGINE_SNOWFLAKE,
    dwh_conn_id="dwh_baesiq",
    start_date=datetime(2026, 2, 20),
    el_operator=EWAHZalandoZDirectOperator,
    operator_config={
        "general_config": {
            "source_conn_id": "zalando_zdirect",
            "params": {"slack_channel": "baesiq"},
            "batch_size": 10000,
        },
        "tables": table_configs,
    },
    target_schema_name="ZALANDO_ZDIRECT",
    target_schema_suffix="_NEXT",
    default_args=default_args,
    schedule_interval="45 3 * * *",
    dagrun_timeout_factor=1,
    # Read the per-run timeout from an Airflow Variable so it can be bumped for a
    # full-load backfill (16h) and dropped back to a normal-incremental ceiling (4h)
    # without a code change.
    additional_dag_args={
        "max_active_tasks": 1,
        "dagrun_timeout": timedelta(hours=_dagrun_timeout_hours),
    },
)[0]

# ORDER OF EXECUTION:
# ORDER_ITEMS reads from the ORDERS _NEXT table on its first run, and ORDER_ITEM_LINES
# reads from ORDER_ITEMS. Enforce the chain explicitly so the downstream tables don't
# race ahead of their dependency. ITEM_QUANTITY_SNAPSHOTS is independent and stays parallel.
zalando_dag.get_task("extract_load_ORDERS") >> \
    zalando_dag.get_task("extract_load_ORDER_ITEMS") >> \
    zalando_dag.get_task("extract_load_ORDER_ITEM_LINES")
