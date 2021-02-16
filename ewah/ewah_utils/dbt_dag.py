from airflow import DAG
from airflow.configuration import conf
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from ewah.constants import EWAHConstants as EC
from ewah.ewah_utils.airflow_utils import EWAHSqlSensor, datetime_utcnow_with_tz
from ewah.ewah_utils.dbt_operator import EWAHdbtOperator
from ewah.hooks.base import EWAHBaseHook as BaseHook

from datetime import datetime, timedelta

import os
import pytz


def dbt_dags_factory_legacy(
    dwh_engine,
    dwh_conn_id,
    project_name,
    dbt_schema_name,
    airflow_conn_id,
    dag_base_name="DBT_run",
    analytics_reader=None,  # list of users of DWH who are read-only
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2019, 1, 1),
    default_args=None,
    folder=None,
    models=None,
    exclude=None,
):

    if analytics_reader:
        for statement in (
            "insert",
            "update",
            "delete",
            "drop",
            "create",
            "select",
            ";",
            "grant",
        ):
            for reader in analytics_reader:
                if statement in reader.lower():
                    raise Exception(
                        "Error! The analytics reader {0} "
                        + "is invalid.".format(reader)
                    )

        # analytics_reader = analytics_reader.split(',')
        analytics_reader_sql = f'\nGRANT USAGE ON SCHEMA "{dbt_schema_name}"'
        analytics_reader_sql += ' TO "{0}";'
        analytics_reader_sql += (
            f'''
        \nGRANT SELECT ON ALL TABLES IN SCHEMA "{dbt_schema_name}"'''
            + ' TO "{0}";'
        )
        analytics_reader_sql = "".join(
            [analytics_reader_sql.format(i) for i in analytics_reader]
        )

    if models and not (type(models) == str):
        models = " --models " + " ".join(models)
    else:
        models = ""

    if exclude and not (type(exclude) == str):
        exclude = " --exclude " + " ".join(exclude)
    else:
        exclude = ""

    flags = models + exclude

    dag = DAG(
        dag_base_name,
        catchup=False,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        start_date=start_date,
        default_args=default_args,
    )

    dag_full_refresh = DAG(
        dag_base_name + "_full_refresh",
        catchup=False,
        max_active_runs=1,
        schedule_interval=None,
        start_date=start_date,
        default_args=default_args,
    )

    folder = folder or (
        os.environ.get("AIRFLOW_HOME") or conf.get("core", "airflow_home")
    ).replace(
        "airflow_home/airflow",
        "dbt_home",
    )

    bash_command = """
    cd {1}
    source env/bin/activate
    cd {2}
    dbt {0}
    """.format(
        "{0}",
        folder,
        project_name,
    )

    sensor_sql = """
        SELECT
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END -- only run if exatly equal to 0
        FROM public.dag_run
        WHERE dag_id IN ('{0}', '{1}')
        and state = 'running'
        and not (run_id = '{2}')
    """.format(
        dag._dag_id,
        dag_full_refresh._dag_id,
        "{{ run_id }}",
    )

    # refactor?! not coupled to values in profiles.yml!
    if dwh_engine == EC.DWH_ENGINE_POSTGRES:
        conn = BaseHook.get_connection(dwh_conn_id)
        env = {
            "DBT_DWH_HOST": str(conn.host),
            "DBT_DWH_USER": str(conn.login),
            "DBT_DWH_PASS": str(conn.password),
            "DBT_DWH_PORT": str(conn.port),
            "DBT_DWH_DBNAME": str(conn.schema),
            "DBT_DWH_SCHEMA": dbt_schema_name,
            "DBT_PROFILES_DIR": folder,
        }
    elif dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
        analytics_conn = BaseHook.get_connection(dwh_conn_id)
        analytics_conn_extra = analytics_conn.extra_dejson
        env = {
            "DBT_ACCOUNT": analytics_conn_extra.get(
                "account",
                analytics_conn.host,
            ),
            "DBT_USER": analytics_conn.login,
            "DBT_PASS": analytics_conn.password,
            "DBT_ROLE": analytics_conn_extra.get("role"),
            "DBT_DB": analytics_conn_extra.get("database"),
            "DBT_WH": analytics_conn_extra.get("warehouse"),
            "DBT_SCHEMA": dbt_schema_name,
            "DBT_PROFILES_DIR": folder,
        }
    else:
        raise ValueError("DWH type not implemented!")

    # with dag:
    snsr = EWAHSqlSensor(
        task_id="sense_dbt_conflict_avoided",
        conn_id=airflow_conn_id,
        sql=sensor_sql,
        poke_interval=5 * 60,
        mode="reschedule",  # don't block a worker and pool slot
        dag=dag,
    )

    dbt_seed = BashOperator(
        task_id="run_dbt_seed",
        bash_command=bash_command.format("seed"),
        env=env,
        dag=dag,
    )

    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command=bash_command.format("run" + flags),
        env=env,
        dag=dag,
    )

    dbt_test = BashOperator(
        task_id="test_dbt",
        bash_command=bash_command.format("test" + flags),
        env=env,
        dag=dag,
    )

    dbt_docs = BashOperator(
        task_id="create_dbt_docs",
        bash_command=bash_command.format("docs generate"),
        env=env,
        dag=dag,
    )

    snsr >> dbt_seed >> dbt_run >> dbt_test

    if analytics_reader:
        # This should not occur when using Snowflake
        read_rights = PostgresOperator(
            task_id="grant_access_to_read_users",
            sql=analytics_reader_sql,
            postgres_conn_id=dwh_conn_id,
            dag=dag,
        )
        dbt_test >> read_rights >> dbt_docs
    else:
        dbt_test >> dbt_docs

    # with dag_full_refresh:
    snsr = EWAHSqlSensor(
        task_id="sense_dbt_conflict_avoided",
        conn_id=airflow_conn_id,
        sql=sensor_sql,
        poke_interval=5 * 60,
        mode="reschedule",  # don't block a worker and pool slot
        dag=dag_full_refresh,
    )

    dbt_seed = BashOperator(
        task_id="run_dbt_seed",
        bash_command=bash_command.format("seed"),
        env=env,
        dag=dag_full_refresh,
    )

    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command=bash_command.format("run --full-refresh" + flags),
        env=env,
        dag=dag_full_refresh,
    )

    dbt_test = BashOperator(
        task_id="test_dbt",
        bash_command=bash_command.format("test" + flags),
        env=env,
        dag=dag_full_refresh,
    )

    dbt_docs = BashOperator(
        task_id="create_dbt_docs",
        bash_command=bash_command.format("docs generate"),
        env=env,
        dag=dag_full_refresh,
    )

    snsr >> dbt_seed >> dbt_run >> dbt_test

    if analytics_reader:
        read_rights = PostgresOperator(
            task_id="grant_access_to_read_users",
            sql=analytics_reader_sql,
            postgres_conn_id=dwh_conn_id,
            dag=dag_full_refresh,
        )
        dbt_test >> read_rights >> dbt_docs
    else:
        dbt_test >> dbt_docs
    return (dag, dag_full_refresh)


def dbt_dag_factory_new(
    airflow_conn_id,
    repo_type,
    dwh_engine,
    dwh_conn_id,
    database_name=None,
    git_conn_id=None,  # if provided, expecting private SSH key in conn extra
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
):
    run_flags = run_flags or ""  # use empty string instead of None

    # only PostgreSQL & Snowflake implemented as of now!
    assert dwh_engine in (EC.DWH_ENGINE_POSTGRES, EC.DWH_ENGINE_SNOWFLAKE)

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
    }
    dag_1 = DAG(dag_base_name, schedule_interval=schedule_interval, **dag_kwargs)
    dag_2 = DAG(dag_base_name + "_full_refresh", schedule_interval=None, **dag_kwargs)

    sensor_sql = """
        SELECT
            -- only run if exactly equal to 0
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
        FROM public.dag_run
        WHERE dag_id IN ('{0}', '{1}')
          AND state = 'running'
          AND not (run_id = '{2}')
          AND (( -- Avoid deadlocks and prioritize scheduled over manual triggers
              run_type = 'scheduled'
              AND execution_date < '{3}' -- execution_date
            ) OR (
              run_type = 'manual'
              AND execution_date < '{4}' -- next_execution_date
          ))
          -- Note: next_execution_date = execution_date if run_type = 'manual'
    """.format(
        dag_1._dag_id,
        dag_2._dag_id,
        "{{ run_id }}",
        "{{ execution_date }}",
        "{{ next_execution_date }}",
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
        "dbt_version": dbt_version,
        "subfolder": subfolder,
        "threads": threads,
        "schema_name": schema_name,
        "keepalives_idle": 0,
        "dwh_engine": dwh_engine,
        "database_name": database_name,
    }

    run_1 = EWAHdbtOperator(
        task_id="dbt_run",
        dbt_commands=["seed", f"run {run_flags}"],
        dag=dag_1,
        **dbt_kwargs,
    )
    run_2 = EWAHdbtOperator(
        task_id="dbt_run",
        dbt_commands=["seed", f"run --full-refresh {run_flags}"],
        dag=dag_2,
        **dbt_kwargs,
    )

    test_1 = EWAHdbtOperator(
        task_id="dbt_test", dbt_commands="test", dag=dag_1, **dbt_kwargs
    )
    test_2 = EWAHdbtOperator(
        task_id="dbt_test", dbt_commands="test", dag=dag_2, **dbt_kwargs
    )

    snsr_1 >> run_1 >> test_1
    snsr_2 >> run_2 >> test_2

    return (dag_1, dag_2)


def dbt_dags_factory(*args, **kwargs):
    if "dbt_schema_name" in kwargs:
        return dbt_dags_factory_legacy(*args, **kwargs)
    else:
        return dbt_dag_factory_new(*args, **kwargs)


def dbt_snapshot_dag(
    dwh_engine,
    dwh_conn_id,
    git_conn_id,
    database_name=None,
    dbt_version="0.18.1",
    subfolder=None,
    threads=4,
    schema_name="analytics",  # for the profiles.yml
    keepalives_idle=0,
    dag_name="T_dbt_snapshots",
    schedule_interval=timedelta(hours=1),
    start_date=None,
    default_args=None,
):
    # only PostgreSQL & Snowflake implemented as of now!
    assert dwh_engine in (EC.DWH_ENGINE_POSTGRES, EC.DWH_ENGINE_SNOWFLAKE)

    dag = DAG(
        dag_name,
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,
        start_date=start_date,
        default_args=default_args,
    )

    task = EWAHdbtOperator(
        dag=dag,
        task_id="dbt_snapshot",
        dbt_commands=["snapshot"],
        repo_type="git",
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        git_conn_id=git_conn_id,
        database_name=database_name,
        dbt_version=dbt_version,
        subfolder=subfolder,
        threads=threads,
        schema_name=schema_name,
        keepalives_idle=keepalives_idle,
    )

    return dag
