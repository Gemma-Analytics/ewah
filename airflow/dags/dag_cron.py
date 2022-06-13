# from airflow import DAG
from ewah.constants import EWAHConstants as EC
from ewah.dag_factories.dag_factory_atomic import dag_factory_atomic
from ewah.operators.hubspot import EWAHHubspotOperator

from datetime import datetime, timedelta
import pytz

tables_config = {
    "deals": {
        "associations": ["line_items"],
        "properties": [
            "amount",
            "dealstage",
            "dealtype",
            "ignoreme",
        ],
    }
}

factory_kwargs = {
    "dwh_engine": EC.DWH_ENGINE_POSTGRES,
    "dwh_conn_id": "dwh",
    "start_date": datetime(2022, 1, 1),
    "el_operator": EWAHHubspotOperator,
    "target_schema_suffix": "_NEXT",
    "schedule_interval": "0 0 * * 1-5",  # Runs every day during the week
    "default_args": {
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_retry": False,
        "email_on_failure": False,
        "email": [],
        "owner": "EWAH",
    },
}

dag_hubspot = dag_factory_atomic(
    dag_name="EL_Hubspot_Cron",
    target_schema_name="raw_hubspot_cron",
    operator_config={
        "general_config": {
            "source_conn_id": "hubspot",
        },
        "tables": tables_config,
    },
    **factory_kwargs
)[0]
