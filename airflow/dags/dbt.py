from airflow import DAG

from ewah.ewah_utils.dbt_dag import dbt_dags_factory, dbt_snapshot_dag
from ewah.constants import EWAHConstants as EC

from datetime import datetime, timedelta

dag1, dag2 = dbt_dags_factory(
    airflow_conn_id='airflow',
    repo_type='git',
    dwh_engine=EC.DWH_ENGINE_POSTGRES,
    dwh_conn_id='dwh',
    git_conn_id='github',
    dbt_version='0.18.1',
    git_link='git@github.com:fishtown-analytics/jaffle_shop.git',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2020,7,22),
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'email': ['email@email.com'],
        'email_on_failure': True,
        'email_on_retry': 'False',
        'owner': 'Data Engineering',
    },
)


dag3, dag4 = dbt_dags_factory(
    dag_base_name='SSH_dbt',
    ssh_tunnel_id='ssh_target',
    airflow_conn_id='airflow',
    repo_type='git',
    dwh_engine=EC.DWH_ENGINE_POSTGRES,
    dwh_conn_id='ssh_dwh',
    git_conn_id='github',
    dbt_version='0.18.1',
    git_link='git@github.com:fishtown-analytics/jaffle_shop.git',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2020,7,22),
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'email': ['email@email.com'],
        'email_on_failure': True,
        'email_on_retry': 'False',
        'owner': 'Data Engineering',
    },
)

dag5, dag6 = dbt_dags_factory(
    dag_base_name='dbt_Snowflake',
    # ssh_tunnel_id='ssh_target',
    airflow_conn_id='airflow',
    repo_type='git',
    dwh_engine=EC.DWH_ENGINE_SNOWFLAKE,
    dwh_conn_id='dwh_snowflake_dbt',
    database_name='EWAH_TESTS',
    git_conn_id='github',
    dbt_version='0.18.1',
    git_link='git@github.com:fishtown-analytics/jaffle_shop.git',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2020,7,22),
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'email': ['email@email.com'],
        'email_on_failure': True,
        'email_on_retry': 'False',
        'owner': 'Data Engineering',
    },
)

snapshot_dag = dbt_snapshot_dag(
    dag_name='T_dbt_snapshots',
    dwh_engine=EC.DWH_ENGINE_POSTGRES,
    dwh_conn_id='dwh',
    git_conn_id='github',
    git_link='git@github.com:fishtown-analytics/jaffle_shop.git',
    dbt_version='0.18.1',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2020,7,22),
    default_args={
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        'email': ['email@email.com'],
        'email_on_failure': True,
        'email_on_retry': 'False',
        'owner': 'Data Engineering',
    },
)
