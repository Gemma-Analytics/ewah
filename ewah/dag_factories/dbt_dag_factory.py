from airflow import DAG

from ewah.constants import EWAHConstants as EC
from ewah.utils.airflow_utils import EWAHSqlSensor, datetime_utcnow_with_tz
from ewah.utils.dbt_operator import EWAHdbtOperator

from croniter import croniter
from datetime import datetime, timedelta
from typing import Optional, Union

import pytz


def dbt_dags_factory(
    airflow_conn_id,
    repo_type,
    dwh_engine,
    dwh_conn_id,
    database_name=None,
    git_conn_id=None,  # if provided, expecting private SSH key in conn extra
    local_path=None,
    dbt_version=None,  # Defaults to require-dbt-version in dbt_project.yml
    subfolder=None,  # optional: supply if dbt project is in a subfolder
    threads=4,  # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-threads
    schema_name="analytics",  # see https://docs.getdbt.com/dbt-cli/configure-your-profile/#understanding-target-schemas
    keepalives_idle=0,  # see https://docs.getdbt.com/reference/warehouse-profiles/postgres-profile/
    dag_base_name="T_dbt_run",
    schedule_interval: Optional[Union[str, timedelta]] = timedelta(days=1),
    full_refresh_schedule_interval: Optional[Union[str, timedelta]] = None,
    start_date=datetime(2019, 1, 1),
    default_args=None,
    run_flags=None,  # e.g. --model tag:base
    seed_flags=None,
    project=None,  # BigQuery alias
    dataset=None,  # BigQuery alias
    dagrun_timeout_factor=None,  # doesn't apply to full refresh
    task_timeout_factor=0.8,  # doesn't apply to full refresh
    metabase_conn_id=None,  # push docs to Metabase after full refresh run if exists
    env_var_conn_ids=None,
):
    run_flags = run_flags or ""  # use empty string instead of None
    seed_flags = seed_flags or ""

    # only PostgreSQL & Snowflake implemented as of now!
    assert dwh_engine in (
        EC.DWH_ENGINE_POSTGRES,
        EC.DWH_ENGINE_SNOWFLAKE,
        EC.DWH_ENGINE_BIGQUERY,
    )

    # Initialize parameters with default values here that can be updated depending
    # on schedule_interval conditions
    catchup = False
    end_date = None
    # Keep original start_date for full refresh DAG
    full_refresh_start_date = start_date

    # Allow using cron-style schedule intervals
    if isinstance(schedule_interval, str):
        assert croniter.is_valid(
            schedule_interval
        ), "schedule_interval is not valid a cron expression!"
    elif isinstance(schedule_interval, timedelta):
        # if start_date is timezone offset-naive, assume utc and turn into offset-aware
        catchup = True
        if not start_date.tzinfo:
            start_date = start_date.replace(tzinfo=pytz.utc)

        start_date += (
            int((datetime_utcnow_with_tz() - start_date) / schedule_interval) - 1
        ) * schedule_interval
        end_date = start_date + 2 * schedule_interval - timedelta(seconds=1)

    dag_kwargs = {
        "catchup": catchup,
        "start_date": start_date,
        "end_date": end_date,
        "default_args": default_args,
        "max_active_runs": 1,
    }

    if dagrun_timeout_factor and isinstance(schedule_interval, timedelta):
        _msg = "dagrun_timeout_factor must be a number between 0 and 1!"
        assert isinstance(dagrun_timeout_factor, (int, float)) and (
            0 < dagrun_timeout_factor <= 1
        ), _msg
        dagrun_timeout = dagrun_timeout_factor * schedule_interval
    else:
        dagrun_timeout = None

    if task_timeout_factor and isinstance(schedule_interval, timedelta):
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
    # for full refresh we need to set the original start_date
    dag_kwargs["start_date"] = full_refresh_start_date
    dag_kwargs["catchup"] = False # dont catch up full refreshes
    dag_2 = DAG(dag_base_name + "_full_refresh", schedule_interval=full_refresh_schedule_interval, **dag_kwargs)

    sensor_sql = """
        SELECT
            -- only succeed if there is no other running DagRun
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
        FROM public.dag_run
        WHERE dag_id IN ('{0}', '{1}')
          AND state = 'running'
          AND data_interval_end < '{2}' -- DagRun's data_interval_end
          AND NOT (run_id = '{3}' AND dag_id = '{4}')
          -- Note: data_interval_end = data_interval_start if run_type = 'manual'
    """.format(
        dag_1._dag_id,
        dag_2._dag_id,
        "{{ data_interval_end }}",
        "{{ run_id }}",
        "{{ dag._dag_id }}",
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
        "env_var_conn_ids": env_var_conn_ids,
    }

    run_1 = EWAHdbtOperator(
        task_id="dbt_run",
        dbt_commands=[f"seed {seed_flags}", f"run {run_flags}"],
        dag=dag_1,
        execution_timeout=execution_timeout,
        **dbt_kwargs,
    )
    run_2 = EWAHdbtOperator(
        task_id="dbt_run",
        dbt_commands=[
            f"seed --full-refresh {seed_flags}",
            f"run --full-refresh {run_flags}",
        ],
        dag=dag_2,
        # If metabase_conn_id exists, push dbt docs to Metabase after full refresh run
        metabase_conn_id=metabase_conn_id,
        **dbt_kwargs,
    )

    run_flags_freshness = (
        run_flags.replace("--models", "--select")
        .replace("--model", "--select")
        .replace("-m ", "-s ")
    )
    test_1 = EWAHdbtOperator(
        task_id="dbt_test",
        dbt_commands=[f"test {run_flags}", f"source freshness {run_flags_freshness}"],
        dag=dag_1,
        execution_timeout=execution_timeout,
        **dbt_kwargs,
    )
    test_2 = EWAHdbtOperator(
        task_id="dbt_test", dbt_commands=f"test {run_flags}", dag=dag_2, **dbt_kwargs
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
    dbt_version=None,
    subfolder=None,
    threads=4,
    schema_name="analytics",  # for the profiles.yml
    keepalives_idle=0,
    dag_name="T_dbt_snapshots",
    schedule_interval: Optional[Union[str, timedelta]] = timedelta(hours=1),
    start_date=None,
    default_args=None,
    dagrun_timeout_factor=None,
    task_timeout_factor=0.8,
    project=None,  # BigQuery alias
    dataset=None,  # BigQuery alias
    run_flags=None,
    env_var_conn_ids=None,
):
    run_flags = run_flags or ""

    # only PostgreSQL & Snowflake implemented as of now!
    assert dwh_engine in (
        EC.DWH_ENGINE_POSTGRES,
        EC.DWH_ENGINE_SNOWFLAKE,
        EC.DWH_ENGINE_BIGQUERY,
    )

    if dagrun_timeout_factor and isinstance(schedule_interval, timedelta):
        _msg = "dagrun_timeout_factor must be a number between 0 and 1!"
        assert isinstance(dagrun_timeout_factor, (int, float)) and (
            0 < dagrun_timeout_factor <= 1
        ), _msg
        dagrun_timeout = dagrun_timeout_factor * schedule_interval
    else:  # In case of 0 set to None
        dagrun_timeout = None

    if task_timeout_factor and isinstance(schedule_interval, timedelta):
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
        dbt_commands=[f"snapshot {run_flags}"],
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
        project=project,
        dataset=dataset,
        env_var_conn_ids=env_var_conn_ids,
    )

    return dag
