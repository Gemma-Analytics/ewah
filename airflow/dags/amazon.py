# from airflow import DAG
from ewah.constants import EWAHConstants as EC
from ewah.dag_factories.dag_factory_atomic import dag_factory_atomic
from ewah.operators.amazon_seller_central import (
    EWAHAmazonSellerCentralReportsAPIOperator as operator,
)

from datetime import datetime, timedelta, date
import pytz

factory_kwargs = {
    "dwh_engine": EC.DWH_ENGINE_POSTGRES,
    "dwh_conn_id": "dwh",
    "start_date": datetime(2022, 1, 1, tzinfo=pytz.utc),
    "el_operator": operator,
    "schedule_interval": timedelta(days=1),  # Runs every day during the week
    "default_args": {
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "email_on_retry": False,
        "email_on_failure": False,
        "email": [],
        "owner": "EWAH",
    },
}

dag_sp_api_fr = dag_factory_atomic(
    dag_name="EL_SP_API_FR",
    target_schema_name="raw_sp_api_reporting_fr",
    operator_config={
        "general_config": {
            "source_conn_id": "seller_central_nl",
            "marketplace_region": "DE",
            # "reload_data_from": datetime(2022, 7, 22, tzinfo=pytz.utc),
        },
        "tables": {
            "listings": {
                "report_name": "listings",
            },
        },
    },
    **factory_kwargs
)[0]
