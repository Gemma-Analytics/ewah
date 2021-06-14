"""This data loading strategy is a mix of full refresh and incremental.
It has two DAGs like the incremental strategy, but no Reset.
Both DAGs have a schedule interval.
There is a Full Refresh DAG and an Incremental DAG.

The Full Refresh DAG runs with a longer periodicity, e.g. 1 day or 1 week.
The Incremental DAG then runs multiple times within the longer periodicity,
e.g. every hour of the day. That way data is current at short time intervals,
but events that can only be captured by a Full Refresh (e.g. deletions) are
also captured.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator


from ewah.constants import EWAHConstants as EC
from ewah.dag_factories.dag_factory_idempotent import ExtendedETS
from ewah.ewah_utils.airflow_utils import (
    PGO,
    etl_schema_tasks,
    datetime_utcnow_with_tz,
    EWAHSqlSensor,
)
from ewah.operators.base import EWAHBaseOperator
from ewah.hooks.base import EWAHBaseHook
from ewah.uploaders.snowflake import SnowflakeOperator

from datetime import datetime, timedelta
from collections.abc import Iterable
from copy import deepcopy
from typing import Optional, Type, Callable, List, Tuple, Union

import re


def dag_factory_mixed(
    dag_name: str,
    dwh_engine: str,
    dwh_conn_id: str,
    airflow_conn_id: str,
    start_date: datetime,
    el_operator: Type[EWAHBaseOperator],
    operator_config: dict,
    target_schema_name: str,
    target_schema_suffix: str = "_next",
    target_database_name: Optional[str] = None,
    default_args: Optional[dict] = None,
    schedule_interval_full_refresh: timedelta = timedelta(days=1),
    schedule_interval_incremental: timedelta = timedelta(hours=1),
    end_date: Optional[datetime] = None,
    read_right_users: Optional[Union[List[str], str]] = None,
    additional_dag_args: Optional[dict] = None,
    additional_task_args: Optional[dict] = None,
    logging_func: Optional[Callable] = None,
    dagrun_timeout_factor: Optional[float] = None,
    task_timeout_factor: Optional[float] = 0.8,
    **kwargs,
) -> Tuple[DAG, DAG]:
    def raise_exception(msg: str) -> None:
        """Add information to error message before raising."""
        raise Exception("DAG: {0} - Error: {1}".format(dag_name, msg))

    logging_func = logging_func or print

    if kwargs:
        logging_func("unused config: {0}".format(str(kwargs)))

    additional_dag_args = additional_dag_args or {}
    additional_task_args = additional_task_args or {}

    if not read_right_users is None:
        if isinstance(read_right_users, str):
            read_right_users = [u.strip() for u in read_right_users.split(",")]
        if not isinstance(read_right_users, Iterable):
            raise_exception("read_right_users must be an iterable or string!")
    if not isinstance(schedule_interval_full_refresh, timedelta):
        raise_exception("schedule_interval_full_refresh must be timedelta!")
    if not isinstance(schedule_interval_incremental, timedelta):
        raise_exception("schedule_interval_incremental must be timedelta!")
    if schedule_interval_incremental >= schedule_interval_full_refresh:
        _msg = "schedule_interval_incremental must be shorter than "
        _msg += "schedule_interval_full_refresh!"
        raise_exception(_msg)

    """Calculate the datetimes and timedeltas for the two DAGs.

    Full Refresh: The start_date should be chosen such that there is always only
    one DAG execution to be executed at any given point in time.
    See dag_factory_atomic for the same calculation including detailed comments.

    The Incremental DAG starts at the start date + schedule interval of the
    Full Refresh DAG, so that the Incremental executions only happen after
    the Full Refresh execution.
    """
    if not start_date.tzinfo:
        # if no timezone is given, assume UTC
        raise_exception("start_date must be timezone aware!")
    time_now = datetime_utcnow_with_tz()

    if end_date:
        end_date = min(end_date, time_now)
    else:
        end_date = time_now

    if start_date > time_now:
        # Start date for both is in the future
        # start_date_fr = start_date
        # start_date_inc = start_date
        pass
    else:
        start_date += (
            int((end_date - start_date) / schedule_interval_full_refresh)
            * schedule_interval_full_refresh
        )
        if start_date == end_date:
            start_date -= schedule_interval_full_refresh
        else:
            start_date -= schedule_interval_full_refresh
            end_date = (
                start_date + 2 * schedule_interval_full_refresh - timedelta(seconds=1)
            )

        # _td = int((time_now - start_date) / schedule_interval_full_refresh) - 2
        # start_date_fr = start_date + _td * schedule_interval_full_refresh
        # start_date_inc = start_date_fr + schedule_interval_full_refresh

    default_args = default_args or {}
    default_args_fr = deepcopy(default_args)
    default_args_inc = deepcopy(default_args)

    if dagrun_timeout_factor:
        _msg = "dagrun_timeout_factor must be a number between 0 and 1!"
        assert isinstance(dagrun_timeout_factor, (int, float)) and (
            0 < dagrun_timeout_factor <= 1
        ), _msg
        dagrun_timeout_inc = dagrun_timeout_factor * schedule_interval_incremental
        dagrun_timeout_fr = dagrun_timeout_factor * schedule_interval_full_refresh
    else:  # In case of 0 set to None
        dagrun_timeout_inc = None
        dagrun_timeout_fr = None

    if task_timeout_factor:
        _msg = "task_timeout_factor must be a number between 0 and 1!"
        assert isinstance(task_timeout_factor, (int, float)) and (
            0 < task_timeout_factor <= 1
        ), _msg
        execution_timeout_fr = task_timeout_factor * schedule_interval_full_refresh
        execution_timeout_inc = task_timeout_factor * schedule_interval_incremental
    else:
        execution_timeout_fr = None
        execution_timeout_inc = None

    dag_name_fr = dag_name + "_Mixed_Atomic"
    dag_name_inc = dag_name + "_Mixed_Idempotent"
    dags = (
        DAG(
            dag_name_fr,
            start_date=start_date,
            end_date=end_date,
            schedule_interval=schedule_interval_full_refresh,
            catchup=True,
            max_active_runs=1,
            default_args=default_args_fr,
            dagrun_timeout=dagrun_timeout_fr,
            **additional_dag_args,
        ),
        DAG(
            dag_name_inc,
            start_date=start_date + schedule_interval_full_refresh,
            end_date=start_date + 2 * schedule_interval_full_refresh,
            schedule_interval=schedule_interval_incremental,
            catchup=True,
            max_active_runs=1,
            default_args=default_args_inc,
            dagrun_timeout=dagrun_timeout_inc,
            **additional_dag_args,
        ),
        DAG(  # Reset DAG
            dag_name + "_Mixed_Reset",
            start_date=start_date,
            end_date=end_date,
            schedule_interval=None,
            catchup=False,
            max_active_runs=1,
            default_args=default_args,
            **additional_dag_args,
        ),
    )

    # Create reset DAG
    reset_bash_command = " && ".join(  # First pause DAGs, then delete their metadata
        [
            "airflow dags pause {dag_name}_Mixed_Atomic",
            "airflow dags pause {dag_name}_Mixed_Idempotent",
            "airflow dags delete {dag_name}_Mixed_Atomic -y",
            "airflow dags delete {dag_name}_Mixed_Idempotent -y",
        ]
    ).format(dag_name=dag_name)
    reset_task = BashOperator(
        bash_command=reset_bash_command,
        task_id="reset_by_deleting_all_task_instances",
        dag=dags[2],
        **additional_task_args,
    )
    drop_sql = """
        DROP SCHEMA IF EXISTS "{target_schema_name}" CASCADE;
        DROP SCHEMA IF EXISTS "{target_schema_name}{suffix}" CASCADE;
    """.format(
        target_schema_name=target_schema_name,
        suffix=target_schema_suffix,
    )
    if dwh_engine == EC.DWH_ENGINE_POSTGRES:
        drop_task = PGO(
            sql=drop_sql,
            postgres_conn_id=dwh_conn_id,
            task_id="delete_previous_schema_if_exists",
            dag=dags[2],
            **additional_task_args,
        )
    elif dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
        drop_task = SnowflakeOperator(
            sql=drop_sql,
            snowflake_conn_id=dwh_conn_id,
            database=target_database_name,
            task_id="delete_previous_schema_if_exists",
            dag=dags[2],
            **additional_task_args,
        )
    else:
        raise_exception(f'DWH "{dwh_engine}" not implemented for this task!')

    kickoff_fr, final_fr = etl_schema_tasks(
        dag=dags[0],
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        read_right_users=read_right_users,
        execution_timeout=execution_timeout_fr,
        **additional_task_args,
    )

    kickoff_inc, final_inc = etl_schema_tasks(
        dag=dags[1],
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        read_right_users=read_right_users,
        execution_timeout=execution_timeout_inc,
        **additional_task_args,
    )

    sql_fr = """
        SELECT
             -- only run if there are no active DAGs that have to finish first
            CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END
        FROM public.dag_run
        WHERE state = 'running'
          AND (
                (dag_id = '{0}' AND execution_date < '{1}')
            OR  (dag_id = '{2}' AND execution_date < '{3}')
          )
    """.format(
        dags[0]._dag_id,  # fr
        "{{ execution_date }}",  # no previous full refresh, please!
        dags[1]._dag_id,  # inc
        "{{ next_execution_date }}",  # no old incremental running, please!
    )

    # Sense if a previous instance runs OR if any incremental loads run
    # except incremental load of the same time, which is expected and waits
    fr_snsr = EWAHSqlSensor(
        task_id="sense_run_validity",
        conn_id=airflow_conn_id,
        sql=sql_fr,
        dag=dags[0],
        poke_interval=5 * 60,
        mode="reschedule",  # don't block a worker and pool slot
        **additional_task_args,
    )

    # Sense if a previous instance is complete excepts if its the first, then
    # check for a full refresh of the same time
    inc_ets = ExtendedETS(
        task_id="sense_run_validity",
        allowed_states=["success"],
        external_dag_id=dags[1]._dag_id,
        external_task_id=final_inc.task_id,
        execution_delta=schedule_interval_incremental,
        backfill_dag_id=dags[0]._dag_id,
        backfill_external_task_id=final_fr.task_id,
        backfill_execution_delta=schedule_interval_full_refresh,
        dag=dags[1],
        poke_interval=5 * 60,
        mode="reschedule",  # don't block a worker and pool slot
        **additional_task_args,
    )

    fr_snsr >> kickoff_fr
    inc_ets >> kickoff_inc

    for table in operator_config["tables"].keys():
        arg_dict_inc = deepcopy(additional_task_args)
        arg_dict_inc.update(operator_config.get("general_config", {}))
        op_conf = operator_config["tables"][table] or {}
        arg_dict_inc.update(op_conf)
        arg_dict_inc.update(
            {
                "extract_strategy": arg_dict_inc.get(
                    "extract_strategy", EC.ES_INCREMENTAL
                ),
                "task_id": "extract_load_" + re.sub(r"[^a-zA-Z0-9_]", "", table),
                "dwh_engine": dwh_engine,
                "dwh_conn_id": dwh_conn_id,
                "target_table_name": op_conf.get("target_table_name", table),
                "target_schema_name": target_schema_name,
                "target_schema_suffix": target_schema_suffix,
                "target_database_name": target_database_name,
            }
        )
        arg_dict_inc["load_strategy"] = arg_dict_inc.get(
            "load_strategy", EC.DEFAULT_LS_PER_ES[arg_dict_inc["extract_strategy"]]
        )
        arg_dict_fr = deepcopy(arg_dict_inc)
        arg_dict_fr["extract_strategy"] = EC.ES_FULL_REFRESH
        arg_dict_fr["load_strategy"] = EC.LS_INSERT_REPLACE

        arg_dict_fr["execution_timeout"] = execution_timeout_fr
        arg_dict_inc["execution_timeout"] = execution_timeout_inc

        task_fr = el_operator(dag=dags[0], **arg_dict_fr)
        task_inc = el_operator(dag=dags[1], **arg_dict_inc)

        kickoff_fr >> task_fr >> final_fr
        kickoff_inc >> task_inc >> final_inc

    return dags
