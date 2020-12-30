from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import datetime, timedelta

descr = "Runs the ldconfig command, may regularly be required for Oracle DAGs "
descr += "to work properly."

dag = DAG(
    "lconfig_dag",
    default_args={
        "owner": "Data Engineering",
        "retries": 0,
        "retry_delay": timedelta(minutes=2),
        "email_on_retry": False,
        "email_on_failure": True,
        "email": ["email@email.com"],
        "priority_weight": 5,
    },
    schedule_interval=timedelta(minutes=3),
    description=descr,
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2020, 1, 1),
)

task = BashOperator(
    task_id="ldconfig_update",
    bash_command="sudo ldconfig /opt/oracle/instantclient_19_8",
    dag=dag,
)
