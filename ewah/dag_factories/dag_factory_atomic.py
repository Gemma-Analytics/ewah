from airflow import DAG

from ewah.constants import EWAHConstants as EC
from ewah.utils.airflow_utils import datetime_utcnow_with_tz
from ewah.operators.base import EWAHBaseOperator
from ewah.uploaders import get_uploader

from collections.abc import Iterable
from copy import deepcopy
from croniter import croniter
from datetime import datetime, timedelta
from typing import Optional, Type, Callable, List, Tuple, Union

import re


def dag_factory_atomic(
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
    schedule_interval: Optional[Union[str, timedelta]] = timedelta(days=1),
    end_date: Optional[datetime] = None,
    read_right_users: Optional[Union[List[str], str]] = None,
    additional_dag_args: Optional[dict] = None,
    additional_task_args: Optional[dict] = None,
    logging_func: Optional[Callable] = None,
    dagrun_timeout_factor: Optional[float] = None,
    task_timeout_factor: Optional[float] = None,
    **kwargs
) -> Tuple[DAG]:
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

    # Initialize parameter with default value here that can be updated depending
    # on schedule_interval conditions
    catchup = False

    # Allow using cron-style schedule intervals
    if isinstance(schedule_interval, str):
        assert croniter.is_valid(
            schedule_interval
        ), "schedule_interval is not valid a cron expression!"
    elif isinstance(schedule_interval, timedelta):
        catchup = True
        # fake catchup = True: between start_date and end_date is one schedule_interval
        # --> run the full refreshs every schedule_interval at the same time instead of
        # having a drift in execution time!
        if end_date:
            end_date = min(end_date, datetime_utcnow_with_tz())
        else:
            end_date = datetime_utcnow_with_tz()

        start_date += (
            int((end_date - start_date) / schedule_interval) - 1
        ) * schedule_interval

        # case 1: end_date = start_date + schedule_interval
        # if the division result is a precise integer, that implies a definite end_date
        # --> adjust to get exactly one schedule_interval delta between start_date and
        # end_date to have one last run available (that should have run before end_date)

        # case 2: end_date > (start_date + schedule_interval)
        # Airflow executes after data_interval_end - start_date has to be
        # between exactly 1 and below 2 time schedule_interval before end_date!
        # end_date - 2*schedule_interval < start_date <= end_date - schedule_interval

        # Make sure only one execution every runs scheduled but manual triggers work!
        end_date = start_date + 2 * schedule_interval - timedelta(seconds=1)

    if dagrun_timeout_factor:
        _msg = "dagrun_timeout_factor must be a number between 0 and 1!"
        assert isinstance(dagrun_timeout_factor, (int, float)) and (
            0 < dagrun_timeout_factor <= 1
        ), _msg
        # If schedule_interval is a string, we assume it is a cron and set a default
        if isinstance(schedule_interval, str):
            default_timeout = timedelta(hours=1)
            additional_dag_args["dagrun_timeout"] = additional_dag_args.get(
              "dagrun_timeout", default_timeout
            )
        else:
          additional_dag_args["dagrun_timeout"] = additional_dag_args.get(
              "dagrun_timeout", dagrun_timeout_factor * schedule_interval
          )

    if task_timeout_factor:
        additional_task_args["execution_timeout"] = additional_task_args.get(
            "execution_timeout", task_timeout_factor * schedule_interval
        )

    dag = DAG(
        dag_name,
        catchup=catchup,
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        start_date=start_date,
        end_date=end_date,
        **additional_dag_args,
    )

    kickoff, final = get_uploader(dwh_engine).get_schema_tasks(
        dag=dag,
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        read_right_users=read_right_users,
        **additional_task_args,
    )

    base_config = deepcopy(additional_task_args)
    base_config.update(operator_config.get("general_config", {}))
    with dag:
        for table in operator_config["tables"].keys():
            table_config = deepcopy(base_config)
            table_config.update(operator_config["tables"][table] or {})
            table_config.update(
                {
                    "task_id": "extract_load_" + re.sub(r"[^a-zA-Z0-9_]", "", table),
                    "dwh_engine": dwh_engine,
                    "dwh_conn_id": dwh_conn_id,
                    "extract_strategy": table_config.get(  # Default to full refresh
                        "extract_strategy", None
                    )
                    or EC.ES_FULL_REFRESH,  # value can be given as None in table conf
                    "target_table_name": operator_config["tables"][table].get(
                        "target_table_name", table
                    ),
                    "target_schema_name": target_schema_name,
                    "target_schema_suffix": target_schema_suffix,
                    "target_database_name": target_database_name,
                }
            )
            # Atomic DAG only works with full refresh and subsequent strategies!
            assert table_config["extract_strategy"] in (
                EC.ES_FULL_REFRESH,
                EC.ES_SUBSEQUENT,
            )
            table_config["load_strategy"] = table_config.get(
                "load_strategy",
                EC.DEFAULT_LS_PER_ES[table_config["extract_strategy"]],
            )
            table_task = el_operator(**table_config)
            kickoff >> table_task >> final

    return (dag,)
