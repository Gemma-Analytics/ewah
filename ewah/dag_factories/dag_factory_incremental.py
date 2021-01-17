from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator

from ewah.constants import EWAHConstants as EC
from ewah.uploaders.snowflake import SnowflakeOperator
from ewah.ewah_utils.airflow_utils import PGO, etl_schema_tasks, datetime_utcnow_with_tz
from ewah.hooks.base import EWAHBaseHook as BaseHook
from ewah.operators.base import EWAHBaseOperator

from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional, Type, Callable, List, Tuple, Union

import re
import time


class ExtendedETS(ExternalTaskSensor):
    """Extend ETS functionality to support the interplay of backfill and
    incremental DAGs."""

    def __init__(
        self,
        backfill_dag_id: Optional[str] = None,
        backfill_execution_delta: Optional[timedelta] = None,
        backfill_execution_date_fn: Optional[Callable] = None,
        backfill_external_task_id: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:

        self.backfill_dag_id = backfill_dag_id
        self.backfill_execution_delta = backfill_execution_delta
        self.backfill_execution_date_fn = backfill_execution_date_fn
        self.backfill_external_task_id = backfill_external_task_id

        super().__init__(*args, **kwargs)

    def execute(self, context: dict) -> None:

        if context["dag"].start_date == context["execution_date"]:
            # First execution of the DAG.
            if self.backfill_dag_id:
                # Check if the latest backfill ran! --> then run normally
                self.execution_delta = (
                    self.backfill_execution_delta or self.execution_delta
                )
                self.execution_date_fn = (
                    self.backfill_execution_date_fn or self.execution_date_fn
                )
                self.external_task_id = (
                    self.backfill_external_task_id or self.external_task_id
                )
                self.external_dag_id = self.backfill_dag_id
                self.log.info("First instance, looking for previous backfill!")
                super().execute(context)
            else:
                self.log.info(
                    "This is the first execution of the DAG. Thus, "
                    + "the sensor automatically succeeds."
                )
        else:
            super().execute(context)


def dag_factory_incremental_loading(
    dag_name: str,
    dwh_engine: str,
    dwh_conn_id: str,
    start_date: datetime,
    el_operator: Type[EWAHBaseOperator],
    operator_config: dict,
    target_schema_name: str,
    target_schema_suffix: str = "_next",
    target_database_name: Optional[str] = None,
    default_args: Optional[dict] = None,
    schedule_interval_backfill: timedelta = timedelta(days=1),
    schedule_interval_future: timedelta = timedelta(hours=1),
    end_date: Optional[datetime] = None,
    read_right_users: Optional[Union[List[str], str]] = None,
    additional_dag_args: Optional[dict] = None,
    additional_task_args: Optional[dict] = None,
    logging_func: Optional[Callable] = None,
    **kwargs,
) -> Tuple[DAG, DAG, DAG]:
    """Returns a tuple of three DAGs associated with incremental data loading.

    The three DAGs are:
    - Reset DAG
    - Backfill DAG
    - Current DAG

    The Reset DAG pauses the other two DAGs, deletes all DAG statistics and
    data, and deletes all data related to the DAGs from the DWH.

    The Backfill DAG runs in a long schedule interval (e.g. a week) from
    start_date on. Each run of this DAG fetches a relatively long period worth
    of data. The purpose is to Backfill the DWH.

    The Current DAG runs in a short schedule interval (e.g. one hour). It has a
    dynamic start date which is the end of the last full schedule interval
    period of the backfill DAG. This DAG keeps the data in the DWH fresh.

    :param dag_name: Base name of the DAG. The returned DAGs will be named
        after dag_nme with the suffixes "_Incremental_Reset",
        "_Inremental_Backfill", and "_Incremental".
    :param dwh_engine: Type of the DWH (e.g. postgresql).
    :param dwh_conn_id: Airflow connection ID with DWH credentials.
    :param start_date: Start date of the DAGs (i.e. of the Backfill DAG).
    :param el_operator: A subclass of EWAHBaseOperator that is used to load
        the individual tables.
    :param target_schema_name: Name of the schema in the DWH that receives the data.
    :param target_schema_suffix: Suffix used during data loading process. The DAG
        creates a new schema "{target_schema_name}{target_schema_suffix}" during
        loading.
    :param target_database_name: Name of the database (Snowflake) or dataset
        (BigQuery), if applicable.
    :param default_args: A dictionary given to the DAGs as default_args param.
    :param schedule_interval_backfill: The schedule interval of the Backfill
        DAG. Must be at least 1 day. Must be larger than
        schedule_interval_future.
    :param schedule_interval_future: The schedule interval of the Current DAG.
        Must be smaller than schedule_interval_backfill. It is recommended not
        to go below 30 minutes. An appropriate schedule interval can be found
        via trial and error. The Current DAG runtime must be less than this
        param in order for EWAH to work properly.
    :param end_date: Airflow DAG kwarg end_date.
    :param read_right_users: List of strings of users or roles that should
        receive read rights on the loaded tables. Can also be a comma-separated
        string instead of a list of strings.
    :param additional_dag_args: kwargs applied to the DAG. Can be any DAG
        kwarg that is not used directly within the function.
    :param additional_task_args: kwargs applied to the tasks. Can be any Task
        kwarg, although some may be overwritten by the function.
    :param logging_func: Pass a callable for logging output. Defaults to print.
    """

    def raise_exception(msg: str) -> None:
        """Add information to error message before raising."""
        raise Exception("DAG: {0} - Error: {1}".format(dag_name, msg))

    logging_func = logging_func or print

    if kwargs:
        logging_func("unused config: {0}".format(str(kwargs)))

    additional_dag_args = additional_dag_args or {}
    additional_task_args = additional_task_args or {}

    if not isinstance(schedule_interval_future, timedelta):
        raise_exception("Schedule intervals must be datetime.timedelta!")
    if not isinstance(schedule_interval_backfill, timedelta):
        raise_exception("Schedule intervals must be datetime.timedelta!")
    if schedule_interval_backfill < timedelta(days=1):
        raise_exception("Backfill schedule interval cannot be below 1 day!")
    if schedule_interval_backfill < schedule_interval_future:
        raise_exception(
            "Backfill schedule interval must be larger than"
            + " regular schedule interval!"
        )
    if not operator_config.get("tables"):
        raise_exception('Requires a "tables" dictionary in operator_config!')
    if not read_right_users is None:
        if isinstance(read_right_users, str):
            read_right_users = [u.strip() for u in read_right_users.split(",")]
        if not isinstance(read_right_users, Iterable):
            raise_exception("read_right_users must be an iterable or string!")

    current_time = datetime_utcnow_with_tz()
    if not start_date.tzinfo:
        raise_exception("start_date must be timezone aware!")

    # Make switch halfway between latest normal DAG run and the
    #   next_execution_date of the next-to-run backfill DAG
    #   --> no interruption of the system, airflow has time to register
    #   the change, the backfill DAG can run once unimpeded and the
    #   normal DAG can then resume as per normal. Note: in that case,
    #   keep both DAGs active!
    current_time += schedule_interval_future / 2
    # How much time has passed in total between start_date and now?
    switch_absolute_date = current_time - start_date
    # How often could the backfill DAG run in that time frame?
    switch_absolute_date /= schedule_interval_backfill
    switch_absolute_date = int(switch_absolute_date)
    # What is the exact datetime after the last of those runs?
    switch_absolute_date *= schedule_interval_backfill
    switch_absolute_date += start_date
    # --> switch_absolute_date is always in the (recent) past

    # Make sure that the backfill and normal DAG start_date and
    #   schedule_interval calculations were successful and correct
    backfill_timedelta = switch_absolute_date - start_date
    backfill_tasks_count = backfill_timedelta / schedule_interval_backfill

    if end_date:
        backfill_end_date = min(switch_absolute_date, end_date)
    else:
        backfill_end_date = switch_absolute_date

    dags = (
        DAG(  # Current DAG
            dag_name + "_Incremental",
            start_date=switch_absolute_date,
            end_date=end_date,
            schedule_interval=schedule_interval_future,
            catchup=True,
            max_active_runs=1,
            default_args=default_args,
            **additional_dag_args,
        ),
        DAG(  # Backfill DAG
            dag_name + "_Incremental_Backfill",
            start_date=start_date,
            end_date=backfill_end_date,
            schedule_interval=schedule_interval_backfill,
            catchup=True,
            max_active_runs=1,
            default_args=default_args,
            **additional_dag_args,
        ),
        DAG(  # Reset DAG
            dag_name + "_Incremental_Reset",
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
            "airflow dags pause {dag_name}_Incremental",
            "airflow dags pause {dag_name}_Incremental_Backfill",
            "airflow dags delete {dag_name}_Incremental -y",
            "airflow dags delete {dag_name}_Incremental_Backfill -y",
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

    reset_task >> drop_task

    # Incremental DAG schema tasks
    kickoff, final = etl_schema_tasks(
        dag=dags[0],
        dwh_engine=dwh_engine,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        dwh_conn_id=dwh_conn_id,
        read_right_users=read_right_users,
        **additional_task_args,
    )

    # Backfill DAG schema tasks
    kickoff_backfill, final_backfill = etl_schema_tasks(
        dag=dags[1],
        dwh_engine=dwh_engine,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        dwh_conn_id=dwh_conn_id,
        read_right_users=read_right_users,
        **additional_task_args,
    )

    # Make sure incremental loading stops if there is an error!
    ets = (
        ExtendedETS(
            task_id="sense_previous_instance",
            allowed_states=["success", "skipped"],
            external_dag_id=dags[0]._dag_id,
            external_task_id=final.task_id,
            execution_delta=schedule_interval_future,
            backfill_dag_id=dags[1]._dag_id,
            backfill_external_task_id=final_backfill.task_id,
            backfill_execution_delta=schedule_interval_backfill,
            dag=dags[0],
            poke_interval=5 * 60,
            mode="reschedule",  # don't block a worker and pool slot
            **additional_task_args,
        ),
        ExtendedETS(
            task_id="sense_previous_instance",
            allowed_states=["success", "skipped"],
            external_dag_id=dags[1]._dag_id,
            external_task_id=final_backfill.task_id,
            execution_delta=schedule_interval_backfill,
            dag=dags[1],
            poke_interval=5 * 60,
            mode="reschedule",  # don't block a worker and pool slot
            **additional_task_args,
        ),
    )
    ets[0] >> kickoff
    ets[1] >> kickoff_backfill

    # add table creation tasks
    arg_dict = deepcopy(additional_task_args)
    arg_dict.update(operator_config.get("general_config", {}))
    # Default reload_data_from to start_date
    arg_dict["reload_data_from"] = arg_dict.get("reload_data_from", start_date)
    for table in operator_config["tables"].keys():
        kwargs = deepcopy(arg_dict)
        kwargs.update(operator_config["tables"][table] or {})

        # Overwrite / ignore changes to these kwargs:
        kwargs.update(
            {
                "extract_strategy": EC.ES_INCREMENTAL,
                "task_id": "extract_load_" + re.sub(r"[^a-zA-Z0-9_]", "", table),
                "dwh_engine": dwh_engine,
                "dwh_conn_id": dwh_conn_id,
                "target_table_name": operator_config["tables"][table].get(
                    "target_table_name", table
                ),
                "target_schema_name": target_schema_name,
                "target_schema_suffix": target_schema_suffix,
                "target_database_name": target_database_name,
            }
        )

        task_backfill = el_operator(dag=dags[1], **kwargs)
        kickoff_backfill >> task_backfill >> final_backfill

        task = el_operator(dag=dags[0], **kwargs)
        kickoff >> task >> final

    return dags
