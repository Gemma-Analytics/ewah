from airflow import DAG

from ewah.ewah_utils.airflow_utils import etl_schema_tasks
from ewah.constants import EWAHConstants as EC

from datetime import datetime, timedelta
from collections.abc import Iterable
from copy import deepcopy
import re

def dag_factory_drop_and_replace(
        dag_name,
        dwh_engine,
        dwh_conn_id,
        el_operator,
        operator_config,
        target_schema_name,
        target_schema_suffix='_next',
        target_database_name=None,
        default_args=None,
        start_date=datetime(2019, 1, 1),
        schedule_interval=timedelta(days=1),
        end_date=None,
        read_right_users=None,
        dwh_ssh_tunnel_conn_id=None,
        additional_dag_args=None,
        additional_task_args=None,
    **kwargs):

    if kwargs:
        for key, value in kwargs.items():
            print('unused config: {0}={1}'.format(key, str(value)))

    additional_dag_args = additional_dag_args or {}
    additional_task_args = additional_task_args or {}

    if dwh_ssh_tunnel_conn_id and not dwh_engine == EC.DWH_ENGINE_POSTGRES:
        raise Exception('DWH tunneling only implemented for PostgreSQL DWHs!')
    if not hasattr(el_operator, '_IS_FULL_REFRESH'):
        raise Exception('Invalid operator supplied!')
    if not el_operator._IS_FULL_REFRESH:
        raise Exception('Operator does not support full refreshs!')
    if not read_right_users is None:
        if type(read_right_users) == str:
            read_right_users = read_right_users.split(',')
        if not isinstance(read_right_users, Iterable):
            raise Exception('read_right_users must be an iterable or string!')

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
        copy_schema=False,
        read_right_users=read_right_users,
        ssh_tunnel_conn_id=dwh_ssh_tunnel_conn_id,
        **additional_task_args
    )

    with dag:
        for table in operator_config['tables'].keys():
            table_config = deepcopy(additional_task_args)
            table_config.update(operator_config.get('general_config', {}))
            table_config.update(operator_config['tables'][table] or {})
            table_config.update({
                'task_id': 'extract_load_'+re.sub(r'[^a-zA-Z0-9_]', '', table),
                'dwh_engine': dwh_engine,
                'dwh_conn_id': dwh_conn_id,
                'load_strategy': EC.LS_FULL_REFRESH,
                'target_table_name': operator_config['tables'][table].get('target_table_name', table),
                'target_schema_name': target_schema_name,
                'target_schema_suffix': target_schema_suffix,
                'target_database_name': target_database_name,
                'target_ssh_tunnel_conn_id': dwh_ssh_tunnel_conn_id,
            })
            table_task = el_operator(**table_config)
            kickoff >> table_task >> final

    return dag
