from airflow import DAG

from ewah.ewah_utils.airflow_utils import etl_schema_tasks

from datetime import datetime, timedelta
from copy import deepcopy

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
    ):

    if not hasattr(el_operator, '_IS_FULL_REFRESH'):
        raise Exception('Invalid operator supplied!')
    if not el_operator._IS_FULL_REFRESH:
        raise Exception('Operator does not support full refreshs!')

    dag = DAG(
        dag_name,
        catchup=False,
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        start_date=start_date,
    )

    kickoff, final = etl_schema_tasks(
        dag=dag,
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        copy_schema=False,
    )

    with dag:
        for table in operator_config['tables'].keys():
            table_config = deepcopy(operator_config.get('general_config', {}))
            table_config.update(operator_config['tables'][table] or {})
            table_config.update({
                'task_id': 'extract_load_'+table,
                'dwh_engine': dwh_engine,
                'dwh_conn_id': dwh_conn_id,
                'target_table_name': table,
                'target_schema_name': target_schema_name,
                'target_schema_suffix': target_schema_suffix,
                'target_database_name': target_database_name,
                'drop_and_replace': True,
            })
            table_task = el_operator(**table_config)
            kickoff >> table_task >> final

    return dag
