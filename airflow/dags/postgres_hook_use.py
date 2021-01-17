from airflow import DAG
from airflow.operators.python import PythonOperator

from ewah.hooks.postgres import EWAHPostgresHook

from datetime import datetime


def task_func(conn_id, sql, **context):
    print(conn_id)
    hook = EWAHPostgresHook(conn_id=conn_id)
    print("got hook, getting data")
    print(hook.get_data_from_sql(sql=sql))


with DAG(
    dag_id="Test_PostgreSQL_Hook_Outside_Operator",
    schedule_interval="@once",
    catchup=False,
    start_date=datetime(2020, 1, 1),
) as dag:
    task = PythonOperator(
        task_id="a_task",
        python_callable=task_func,
        op_kwargs={"conn_id": "dwh", "sql": "select 1 as a, 'b' as b"},
    )
