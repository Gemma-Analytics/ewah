from airflow import DAG

from ewah.constants import EWAHConstants as EC
from ewah.utils.airflow_utils import EWAHSqlSensor, datetime_utcnow_with_tz
from ewah.utils.dbt_operator import EWAHdbtOperator

from datetime import datetime, timedelta

import pytz


def dbt_dags_factory(
    airflow_conn_id,
    repo_type,
    dwh_engine,
    dwh_conn_id,
    database_name=None,
    git_conn_id=None,  # if provided, expecting private SSH key in conn extra
    local_path=None,
    dbt_version="0.18.1",
    subfolder=None,  # optional: supply if dbt project is in a subfolder
    threads=4,  # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-threads
    schema_name="analytics",  # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-target-schemas
    keepalives_idle=0,  # see https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile/
    dag_base_name="T_dbt_run",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2019, 1, 1),
    default_args=None,
    run_flags=None,  # e.g. --model tag:base
    project=None,  # BigQuery alias
    dataset=None,  # BigQuery alias
    dagrun_timeout_factor=None,  # doesn't apply to full refresh
    task_timeout_factor=0.8,  # doesn't apply to full refresh
    metabase_conn_id=None,  # push docs to Metabase after full refresh run if exists
):
    run_flags = run_flags or ""  # use empty string instead of None

    # only PostgreSQL & Snowflake implemented as of now!
    assert dwh_engine in (
        EC.DWH_ENGINE_POSTGRES,
        EC.DWH_ENGINE_SNOWFLAKE,
        EC.DWH_ENGINE_BIGQUERY,
    )

    # if start_date is timezone offset-naive, assume utc and turn into offset-aware
    if not start_date.tzinfo:
        start_date = start_date.replace(tzinfo=pytz.utc)

    start_date += (
        int((datetime_utcnow_with_tz() - start_date) / schedule_interval) - 1
    ) * schedule_interval
    end_date = start_date + 2 * schedule_interval - timedelta(seconds=1)

    dag_kwargs = {
        "catchup": True,
        "start_date": start_date,
        "end_date": end_date,
        "default_args": default_args,
        "max_active_runs": 1,
    }

    if dagrun_timeout_factor:
        _msg = "dagrun_timeout_factor must be a number between 0 and 1!"
        assert isinstance(dagrun_timeout_factor, (int, float)) and (
            0 < dagrun_timeout_factor <= 1
        ), _msg
        dagrun_timeout = dagrun_timeout_factor * schedule_interval
    else:
        dagrun_timeout = None

    if task_timeout_factor:
        _msg = "task_timeout_factor must be a number between 0 and 1!"
        assert isinstance(task_timeout_factor, (int, float)) and (
            0 < task_timeout_factor <= 1
        ), _msg
        execution_timeout = task_timeout_factor * schedule_interval
    else:
        execution_timeout = None

    dag_1 = DAG(
        dag_base_name,
        schedule_interval=schedule_interval,
        dagrun_timeout=dagrun_timeout,
        **dag_kwargs,
    )
    dag_2 = DAG(dag_base_name + "_full_refresh", schedule_interval=None, **dag_kwargs)

    sensor_sql = """
        SELECT
            -- only succeed if there is no other running DagRun
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
        FROM public.dag_run
        WHERE dag_id IN ('{0}', '{1}')
          AND state = 'running'
          AND data_interval_end < '{2}' -- DagRun's data_interval_end
          -- Note: data_interval_end = data_interval_start if run_type = 'manual'
    """.format(
        dag_1._dag_id,
        dag_2._dag_id,
        "{{ data_interval_end }}",
    )

    snsr_1 = EWAHSqlSensor(
        task_id="sense_dbt_conflict_avoided",
        conn_id=airflow_conn_id,
        sql=sensor_sql,
        poke_interval=5 * 60,
        mode="reschedule",  # don't block a worker and pool slot
        dag=dag_1,
    )
    snsr_2 = EWAHSqlSensor(
        task_id="sense_dbt_conflict_avoided",
        conn_id=airflow_conn_id,
        sql=sensor_sql,
        poke_interval=5 * 60,
        mode="reschedule",  # don't block a worker and pool slot
        dag=dag_2,
    )

    dbt_kwargs = {
        "repo_type": repo_type,
        "dwh_conn_id": dwh_conn_id,
        "git_conn_id": git_conn_id,
        "local_path": local_path,
        "dbt_version": dbt_version,
        "subfolder": subfolder,
        "threads": threads,
        "schema_name": schema_name,
        "keepalives_idle": 0,
        "dwh_engine": dwh_engine,
        "database_name": database_name,
        "project": project,
        "dataset": dataset,
    }

    run_1 = EWAHdbtOperator(
        task_id="dbt_run",
        dbt_commands=["seed", f"run {run_flags}"],
        dag=dag_1,
        execution_timeout=execution_timeout,
        **dbt_kwargs,
    )
    run_2 = EWAHdbtOperator(
        task_id="dbt_run",
        dbt_commands=["seed --full-refresh", f"run --full-refresh {run_flags}"],
        dag=dag_2,
        # If metabase_conn_id exists, push dbt docs to Metabase after full refresh run
        metabase_conn_id=metabase_conn_id,
        **dbt_kwargs,
    )

    test_1 = EWAHdbtOperator(
        task_id="dbt_test",
        dbt_commands="test",
        dag=dag_1,
        execution_timeout=execution_timeout,
        **dbt_kwargs,
    )
    test_2 = EWAHdbtOperator(
        task_id="dbt_test", dbt_commands="test", dag=dag_2, **dbt_kwargs
    )

    snsr_1 >> run_1 >> test_1
    snsr_2 >> run_2 >> test_2

    return (dag_1, dag_2)


def dbt_snapshot_dag(
    dwh_engine,
    dwh_conn_id,
    repo_type="git",
    database_name=None,
    git_conn_id=None,
    local_path=None,
    dbt_version="0.18.1",
    subfolder=None,
    threads=4,
    schema_name="analytics",  # for the profiles.yml
    keepalives_idle=0,
    dag_name="T_dbt_snapshots",
    schedule_interval=timedelta(hours=1),
    start_date=None,
    default_args=None,
    dagrun_timeout_factor=None,
    task_timeout_factor=0.8,
):
    # only PostgreSQL & Snowflake implemented as of now!
    assert dwh_engine in (EC.DWH_ENGINE_POSTGRES, EC.DWH_ENGINE_SNOWFLAKE)

    if dagrun_timeout_factor:
        _msg = "dagrun_timeout_factor must be a number between 0 and 1!"
        assert isinstance(dagrun_timeout_factor, (int, float)) and (
            0 < dagrun_timeout_factor <= 1
        ), _msg
        dagrun_timeout = dagrun_timeout_factor * schedule_interval
    else:  # In case of 0 set to None
        dagrun_timeout = None

    if task_timeout_factor:
        _msg = "dagrun_timeout_factor must be a number between 0 and 1!"
        assert isinstance(task_timeout_factor, (int, float)) and (
            0 < task_timeout_factor <= 1
        ), _msg
        execution_timeout = task_timeout_factor * schedule_interval
    else:
        execution_timeout = None

    dag = DAG(
        dag_name,
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,
        start_date=start_date,
        default_args=default_args,
        dagrun_timeout=dagrun_timeout,
    )

    task = EWAHdbtOperator(
        dag=dag,
        task_id="dbt_snapshot",
        dbt_commands=["snapshot"],
        repo_type=repo_type,
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        git_conn_id=git_conn_id,
        local_path=local_path,
        database_name=database_name,
        dbt_version=dbt_version,
        subfolder=subfolder,
        threads=threads,
        schema_name=schema_name,
        keepalives_idle=keepalives_idle,
        execution_timeout=execution_timeout,
    )

    return dag
