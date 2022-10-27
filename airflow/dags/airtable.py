# from airflow import DAG
from ewah.dag_factories.dag_factory_atomic import dag_factory_atomic

from ewah.operators.airtable import EWAHAirtableOperator
from ewah.constants import EWAHConstants
from datetime import timedelta, datetime
import pytz

dwh_engine = EWAHConstants.DWH_ENGINE_SNOWFLAKE
target_schema_suffix = "_NEXT"
dwh_conn_id = "snowflake_conn"
default_args = {}
additional_task_args = {}
schedule_interval = timedelta(hours=1)
start_date = datetime(2022, 10, 1, tzinfo=pytz.utc)

dag = dag_factory_atomic(
    "Snow_EL_Airtable",
    el_operator=EWAHAirtableOperator,
    target_schema_name="AIRTABLE",
    start_date=start_date,
    operator_config={
        "general_config": {
            "source_conn_id": "airtable",
            "base_id": "appgJXhQUfArKhgDy",
        },
        "tables": {
            "projects": {"table_id": "tblWIqEoPXUA00LId"},
        },
    },
    dwh_conn_id=dwh_conn_id,
    dwh_engine=dwh_engine,
    target_schema_suffix=target_schema_suffix,
    default_args=default_args,
    additional_task_args=additional_task_args,
    schedule_interval=schedule_interval,
)[0]
