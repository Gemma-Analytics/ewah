from airflow import DAG

from ewah.constants import EWAHConstants as EC
from ewah.ewah_utils.airflow_utils import etl_schema_tasks
from ewah.operators.base import EWAHBaseOperator

from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Optional, Type, Callable, List, Tuple, Union

import re


def dag_factory_drop_and_replace(
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
    schedule_interval: timedelta = timedelta(days=1),
    end_date: Optional[datetime] = None,
    read_right_users: Optional[Union[List[str], str]] = None,
    dwh_ssh_tunnel_conn_id: Optional[str] = None,
    additional_dag_args: Optional[dict] = None,
    additional_task_args: Optional[dict] = None,
    logging_func: Optional[Callable] = None,
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

    if dwh_ssh_tunnel_conn_id and not dwh_engine == EC.DWH_ENGINE_POSTGRES:
        raise_exception("DWH tunneling only implemented for PostgreSQL DWHs!")
    if not read_right_users is None:
        if isinstance(read_right_users, str):
            read_right_users = [u.strip() for u in read_right_users.split(",")]
        if not isinstance(read_right_users, Iterable):
            raise_exception("read_right_users must be an iterable or string!")

    dag = DAG(
        dag_name,
        catchup=False,
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        start_date=start_date,
        end_date=end_date,
        **additional_dag_args,
    )

    kickoff, final = etl_schema_tasks(
        dag=dag,
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        read_right_users=read_right_users,
        ssh_tunnel_conn_id=dwh_ssh_tunnel_conn_id,
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
                    "extract_strategy": EC.ES_FULL_REFRESH,
                    "target_table_name": operator_config["tables"][table].get(
                        "target_table_name", table
                    ),
                    "target_schema_name": target_schema_name,
                    "target_schema_suffix": target_schema_suffix,
                    "target_database_name": target_database_name,
                    "target_ssh_tunnel_conn_id": dwh_ssh_tunnel_conn_id,
                }
            )
            table_task = el_operator(**table_config)
            kickoff >> table_task >> final

    return (dag,)
