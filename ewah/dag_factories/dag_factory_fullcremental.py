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
from ewah.hooks.base import EWAHBaseHook as BaseHook
from airflow.sensors.sql_sensor import SqlSensor

from ewah.ewah_utils.airflow_utils import etl_schema_tasks
from ewah.constants import EWAHConstants as EC
from ewah.dag_factories.dag_factory_incremental import ExtendedETS

from datetime import datetime, timedelta
from collections.abc import Iterable
from copy import deepcopy
import time
import pytz
import re

def dag_factory_fullcremental(
        dag_base_name,
        dwh_engine,
        dwh_conn_id,
        airflow_conn_id,
        el_operator,
        operator_config,
        target_schema_name,
        target_schema_suffix='_next',
        target_database_name=None,
        default_args=None,
        start_date=datetime(2020,1,1),
        schedule_interval_full_refresh=timedelta(days=1),
        schedule_interval_incremental=timedelta(hours=1),
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
    if not read_right_users is None:
        if type(read_right_users) == str:
            read_right_users = read_right_users.split(',')
        if not isinstance(read_right_users, Iterable):
            raise Exception('read_right_users must be an iterable or string!')
    if not isinstance(schedule_interval_full_refresh, timedelta):
        raise Exception('schedule_interval_full_refresh must be timedelta!')
    if not isinstance(schedule_interval_incremental, timedelta):
        raise Exception('schedule_interval_incremental must be timedelta!')
    if schedule_interval_incremental >= schedule_interval_full_refresh:
        _msg = 'schedule_interval_incremental must be shorter than '
        _msg += 'schedule_interval_full_refresh!'
        raise Exception(_msg)

    """Calculate the datetimes and timedeltas for the two DAGs.

    Full Refresh: The start_date should be chosen such that there is always only
    one DAG execution to be executed at any given point in time.

    The Incremental DAG starts at the start date + schedule interval of the
    Full Refresh DAG, so that the Incremental executions only happen after
    the Full Refresh execution.
    """
    if not start_date.tzinfo:
        # if no timezone is given, assume UTC
        start_date = start_date.replace(tzinfo=pytz.utc)
    time_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    time_now += schedule_interval_incremental / 2
    if start_date > time_now:
        start_date_fr = start_date
        start_date_inc = start_date
    else:
        _td = int((time_now - start_date) / schedule_interval_full_refresh) - 1
        start_date_fr = start_date + _td * schedule_interval_full_refresh
        start_date_inc = start_date_fr + schedule_interval_full_refresh

    dag_name_fr = dag_base_name + '_Periodic_Full_Refresh'
    dag_name_inc = dag_base_name + '_Intraperiod_Incremental'
    dags = (
        DAG(dag_name_fr,
            start_date=start_date_fr,
            end_date=end_date,
            schedule_interval=schedule_interval_full_refresh,
            catchup=True,
            max_active_runs=1,
            default_args=default_args,
        **additional_dag_args),
        DAG(dag_name_inc,
            start_date=start_date_inc,
            end_date=end_date,
            schedule_interval=schedule_interval_incremental,
            catchup=True,
            max_active_runs=1,
            default_args=default_args,
        **additional_dag_args),
    )

    kickoff_fr, final_fr = etl_schema_tasks(
        dag=dags[0],
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        read_right_users=read_right_users,
        ssh_tunnel_conn_id=dwh_ssh_tunnel_conn_id,
    **additional_task_args)

    kickoff_inc, final_inc = etl_schema_tasks(
        dag=dags[1],
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        target_schema_name=target_schema_name,
        target_schema_suffix=target_schema_suffix,
        target_database_name=target_database_name,
        read_right_users=read_right_users,
        ssh_tunnel_conn_id=dwh_ssh_tunnel_conn_id,
    **additional_task_args)

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
        dags[0]._dag_id, # fr
        '{{ execution_date }}', # no previous full refresh, please!
        dags[1]._dag_id, # inc
        '{{ next_execution_date }}', # no old incremental running, please!
    )

    # Sense if a previous instance runs OR if any incremental loads run
    # except incremental load of the same time, which is expected and waits
    fr_snsr = SqlSensor(
        task_id='sense_run_validity',
        conn_id=airflow_conn_id,
        sql=sql_fr,
        dag=dags[0],
        poke_interval=5*60,
        mode='reschedule', # don't block a worker and pool slot
    **additional_task_args)

    # Sense if a previous instance is complete excepts if its the first, then
    # check for a full refresh of the same time
    inc_ets = ExtendedETS(
        task_id='sense_run_validity',
        allowed_states=['success'],
        external_dag_id=dags[1]._dag_id,
        external_task_id=final_inc.task_id,
        execution_delta=schedule_interval_incremental,
        backfill_dag_id=dags[0]._dag_id,
        backfill_external_task_id=final_fr.task_id,
        backfill_execution_delta=schedule_interval_full_refresh,
        dag=dags[1],
        poke_interval=5*60,
        mode='reschedule', # don't block a worker and pool slot
    **additional_task_args)

    fr_snsr >> kickoff_fr
    inc_ets >> kickoff_inc

    for table in operator_config['tables'].keys():
        arg_dict_inc = deepcopy(additional_task_args)
        # load_strategy can be overwritten to ES_FULL_REFRESH on per-table level
        arg_dict_inc.update({'load_strategy': EC.ES_INCREMENTAL})
        arg_dict_inc.update(operator_config.get('general_config', {}))
        op_conf = operator_config['tables'][table] or {}
        arg_dict_inc.update(op_conf)
        arg_dict_inc.update({
            'task_id': 'extract_load_' + re.sub(r'[^a-zA-Z0-9_]', '', table),
            'dwh_engine': dwh_engine,
            'dwh_conn_id': dwh_conn_id,
            'target_table_name': op_conf.get('target_table_name', table),
            'target_schema_name': target_schema_name,
            'target_schema_suffix': target_schema_suffix,
            'target_database_name': target_database_name,
            'target_ssh_tunnel_conn_id': dwh_ssh_tunnel_conn_id,
        })
        arg_dict_fr = deepcopy(arg_dict_inc)
        arg_dict_fr.update({'load_strategy': EC.ES_FULL_REFRESH})

        task_fr = el_operator(dag=dags[0], **arg_dict_fr)
        task_inc = el_operator(dag=dags[1], **arg_dict_inc)

        kickoff_fr >> task_fr >> final_fr
        kickoff_inc >> task_inc >> final_inc

    return dags
