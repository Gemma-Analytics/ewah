from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor as ETS

from ewah.ewah_utils.airflow_utils import PGO
from ewah.ewah_utils.airflow_utils import etl_schema_tasks
from ewah.dwhooks.dwhook_snowflake import SnowflakeOperator
from ewah.constants import EWAHConstants as EC

from datetime import datetime, timedelta
from collections.abc import Iterable
from copy import deepcopy
import time
import pytz
import re

class ExtendedETS(ETS):
    """Extend ETS functionality to support the interplay of backfill and
    incremental DAGs."""

    def __init__(self,
        backfill_dag_id=None,
        backfill_execution_delta=None,
        backfill_execution_date_fn=None,
        backfill_external_task_id=None,
        execution_delay_in_seconds=120,
    *args, **kwargs):

        self.backfill_dag_id = backfill_dag_id
        self.backfill_execution_delta = backfill_execution_delta
        self.backfill_execution_date_fn = backfill_execution_date_fn
        self.backfill_external_task_id = backfill_external_task_id
        self.execution_delay_in_seconds = execution_delay_in_seconds

        super().__init__(*args, **kwargs)


    def execute(self, context):

        # When a DAG is executed as soon as possible, some data sources
        # may not immediately have up to date data from their API.
        # E.g. querying all data until 12.30pm only gives all relevant data
        # after 12.32pm due to some internal delays. In those cases, make
        # sure the incremental loading DAGs don't execute too quickly.
        next_execution_date = context['next_execution_date'] # type: Pendulum
        next_execution_date +=timedelta(seconds=self.execution_delay_in_seconds)
        while datetime.now() < next_execution_date:
            self.log.info('Waiting until {0} to execute... (now: {1})'.format(
                str(next_execution_date),
                str(datetime.now())
            ))
            time.sleep((self.execution_delay_in_seconds or 20) / 20)

        if context['dag'].start_date == context['execution_date']:
            # First execution of the DAG.
            if self.backfill_dag_id:
                # Check if the latest backfill ran! --> then run normally
                self.execution_delta = self.backfill_execution_delta or \
                    self.execution_delta
                self.execution_date_fn = self.backfill_execution_date_fn or \
                    self.execution_date_fn
                self.external_task_id = self.backfill_external_task_id or \
                    self.external_task_id
                self.external_dag_id = self.backfill_dag_id
                self.log.info('First instance, looking for previous backfill!')
                super().execute(context)
            else:
                self.log.info('This is the first execution of the DAG. Thus, ' \
                    + 'the sensor automatically succeeds.')
        else:
            super().execute(context)

def dag_factory_incremental_loading(
        dag_base_name,
        dwh_engine,
        dwh_conn_id,
        airflow_conn_id,
        start_date,
        el_operator,
        operator_config,
        target_schema_name,
        target_schema_suffix='_next',
        target_database_name=None,
        default_args=None,
        schedule_interval_backfill=timedelta(days=1),
        schedule_interval_future=timedelta(hours=1),
        switch_absolute_date=None, # If provided, switch from backfill DAG to
        #   normal DAG at that point in time (datetime.datetime) - best practice
        #   would be to leave None is most cases
        switch_relative_timedelta=None, # if switch_absolute_date is None, when
        #   to move from backfill DAG to normal DAG? Defaults to minuts half
        #   of schedule_interval_future (which is recommended in most cases)
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
    if not type(schedule_interval_future) == timedelta:
        raise Exception('Schedule intervals must be datetime.timedelta!')
    if not type(schedule_interval_backfill) == timedelta:
        raise Exception('Schedule intervals must be datetime.timedelta!')
    if schedule_interval_backfill < timedelta(days=1):
        raise Exception('Backfill schedule interval cannot be below 1 day!')
    if schedule_interval_backfill < schedule_interval_future:
        raise Exception('Backfill schedule interval must be larger than' \
            + ' regular schedule interval!')
    if not operator_config.get('tables'):
        raise Exception('Requires a "tables" dictionary in operator_config!')
    if not (switch_relative_timedelta is None \
        or type(switch_relative_timedelta) == timedelta):
        raise Exception('switch_relative_timedelta must be timedelta or None!')
    if not read_right_users is None:
        if type(read_right_users) == str:
            read_right_users = read_right_users.split(',')
        if not isinstance(read_right_users, Iterable):
            raise Exception('read_right_users must be an iterable or string!')


    if not switch_absolute_date:
        if switch_relative_timedelta is None:
            # Make switch halway between latest normal DAG run and the
            #   next_execution_date of the next-to-run backfill DAG
            #   --> no interruption of the system, airflow has time to register
            #   the change, the backfill DAG can run once unimpeded and the
            #   normal DAG can then resume as per normal. Note: in that case,
            #   keep both DAGs active!
            switch_relative_timedelta = -schedule_interval_future / 2

        time_now = datetime.utcnow()
        if start_date.tzinfo:
            time_now = time_now.replace(tzinfo=pytz.utc)

        current_time = time_now - switch_relative_timedelta
        # How much time has passed in total between start_date and now?
        switch_absolute_date = current_time - start_date
        # How often could the backfill DAG run in that time frame?
        switch_absolute_date /= schedule_interval_backfill
        switch_absolute_date = int(switch_absolute_date)
        # What is the exact datetime after the last of those runs?
        switch_absolute_date *= schedule_interval_backfill
        switch_absolute_date += start_date
        # --> switch_absolute_date is always in the (recent) past, unless
        #   switch_relative_timedelta is negative

    # Make sure that the backfill and normal DAG start_date and
    #   schedule_interval calculations were successful and correct
    backfill_timedelta = switch_absolute_date - start_date
    backfill_tasks_count = backfill_timedelta / schedule_interval_backfill
    # The schedule interval of the backfill must be an exact integer multiple
    # of the time period between start date and switch date!
    if not (backfill_tasks_count == round(backfill_tasks_count, 0)):
        raise Exception('The schedule interval of the backfill must be an ' \
            + 'exact integer multiple of the time period between start date '\
            + 'and switch date!')
    if end_date:
        backfill_end_date = min(switch_absolute_date, end_date)
    else:
        backfill_end_date = switch_absolute_date
    dags = (
        DAG(
            dag_base_name+'_Incremental',
            start_date=switch_absolute_date,
            end_date=end_date,
            schedule_interval=schedule_interval_future,
            catchup=True,
            max_active_runs=1,
            default_args=default_args,
            **additional_dag_args
        ),
        DAG(
            dag_base_name+'_Incremental_Backfill',
            start_date=start_date,
            end_date=backfill_end_date,
            schedule_interval=schedule_interval_backfill,
            catchup=True,
            max_active_runs=1,
            default_args=default_args,
            **additional_dag_args
        ),
        DAG(
            dag_base_name+'_Incremental_Reset',
            start_date=start_date,
            end_date=end_date,
            schedule_interval=None,
            catchup=False,
            max_active_runs=1,
            default_args=default_args,
            **additional_dag_args
        ),
    )

    # Create reset DAG
    reset_sql = """
        /*
            Different versions of airflow contain different tables. Only
            DELETE DAG from tables that actually exist.
        */
        CREATE OR REPLACE FUNCTION __ewah_delete_all_dag_stats(dag_name text)
        RETURNS void AS
        $$
        DECLARE
        	meta_db text;
        	meta_schema text;
        	meta_table text;
        BEGIN
        	FOR meta_db, meta_schema, meta_table IN
        		SELECT table_catalog, table_schema, table_name
        		FROM information_schema.tables
        		WHERE table_name IN ('dag_run'
                    , 'job'
                    , 'task_fail'
                    , 'task_instance'
                    , 'task_reschedule'
                    , 'xcom'
                    , 'dag_stats'
                )
        		AND table_catalog LIKE %(db_name)s
        		AND table_schema LIKE %(schema_name)s
        	LOOP
                IF meta_table = 'dag_run' THEN
                    -- Only once per DAG: turn DAGs off!
                    EXECUTE 'UPDATE "' || meta_db || '"."' || meta_schema
                        || '"."dag"'
                        || ' SET is_paused = TRUE'
                        || ' WHERE dag_id = ''' || dag_name || '''';
                END IF;
        		EXECUTE 'DELETE FROM "' || meta_db || '"."' || meta_schema
                    || '"."' || meta_table
                    || '" WHERE dag_id = ''' || dag_name || '''';
        	END LOOP;
        END;
        $$ LANGUAGE plpgsql VOLATILE;
        SELECT __ewah_delete_all_dag_stats(%(dag_name)s);
        SELECT __ewah_delete_all_dag_stats(%(dag_name_backfill)s);
        DROP FUNCTION __ewah_delete_all_dag_stats;
    """
    airflow_conn = BaseHook.get_connection(airflow_conn_id)
    reset_task = PGO(
        sql=reset_sql,
        postgres_conn_id=airflow_conn_id,
        parameters={
            'dag_name': dag_base_name+'_Incremental',
            'dag_name_backfill': dag_base_name+'_Incremental_Backfill',
            'db_name': airflow_conn.schema,
            'schema_name': airflow_conn.extra_dejson.get('schema', 'public'),
        },
        task_id='reset_by_deleting_all_task_instances',
        dag=dags[2],
        **additional_task_args
    )
    drop_sql = f'DROP SCHEMA IF EXISTS "{target_schema_name}" CASCADE;'
    drop_sql += '\nDROP SCHEMA IF EXISTS "{schema}" CASCADE;'.format(**{
        'schema': target_schema_name + target_schema_suffix,
    })
    if dwh_engine == EC.DWH_ENGINE_POSTGRES:
        drop_task = PGO(
            sql=drop_sql,
            postgres_conn_id=dwh_conn_id,
            task_id='delete_previous_schema_if_exists',
            dag=dags[2],
            ssh_tunnel_conn_id=dwh_ssh_tunnel_conn_id,
            **additional_task_args
        )
    elif dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
        drop_task = SnowflakeOperator(
            sql=drop_sql,
            snowflake_conn_id=dwh_conn_id,
            database=target_database_name,
            task_id='delete_previous_schema_if_exists',
            dag=dags[2],
            **additional_task_args
        )
    else:
        raise ValueError('DWH "{0}" not implemented for this task!'.format(
            dwh_engine,
        ))
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
        ssh_tunnel_conn_id=dwh_ssh_tunnel_conn_id,
        **additional_task_args
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
        ssh_tunnel_conn_id=dwh_ssh_tunnel_conn_id,
        **additional_task_args
    )

    # Make sure incremental loading stops if there is an error!
    ets = (
        ExtendedETS(
            task_id='sense_previous_instance',
            allowed_states=['success', 'skipped'],
            external_dag_id=dags[0]._dag_id,
            external_task_id=final.task_id,
            execution_delta=schedule_interval_future,
            backfill_dag_id=dags[1]._dag_id,
            backfill_external_task_id=final_backfill.task_id,
            backfill_execution_delta=schedule_interval_backfill,
            dag=dags[0],
            poke_interval=5*60,
            mode='reschedule', # don't block a worker and pool slot
            **additional_task_args
        ),
        ExtendedETS(
            task_id='sense_previous_instance',
            allowed_states=['success', 'skipped'],
            external_dag_id=dags[1]._dag_id,
            external_task_id=final_backfill.task_id,
            execution_delta=schedule_interval_backfill,
            dag=dags[1],
            poke_interval=5*60,
            mode='reschedule', # don't block a worker and pool slot
            **additional_task_args
        )
    )
    ets[0] >> kickoff
    ets[1] >> kickoff_backfill

    # add table creation tasks
    count_backfill_tasks = 0
    for table in operator_config['tables'].keys():
        arg_dict = deepcopy(additional_task_args)
        arg_dict.update({'load_strategy': EC.LS_INCREMENTAL})
        arg_dict.update(operator_config.get('general_config', {}))
        arg_dict_internal = {
            'task_id': 'extract_load_' + re.sub(r'[^a-zA-Z0-9_]', '', table),
            'dwh_engine': dwh_engine,
            'dwh_conn_id': dwh_conn_id,
            'target_table_name': operator_config['tables'][table].get('target_table_name', table),
            'target_schema_name': target_schema_name,
            'target_schema_suffix': target_schema_suffix,
            'target_database_name': target_database_name,
            'target_ssh_tunnel_conn_id': dwh_ssh_tunnel_conn_id,
        }

        arg_dict_backfill = deepcopy(arg_dict)
        arg_dict.update(operator_config.get('incremental_config', {}))
        arg_dict_backfill.update(operator_config.get('backfill_config', {}))
        arg_dict.update(operator_config['tables'][table] or {})
        arg_dict_backfill.update(operator_config['tables'][table] or {})

        arg_dict.update(arg_dict_internal)
        arg_dict_backfill.update(arg_dict_internal)

        if arg_dict.get('load_strategy') == EC.LS_INCREMENTAL:
            # don't load full refresh tables in backfill
            task_backfill = el_operator(dag=dags[1], **arg_dict_backfill)
            kickoff_backfill >> task_backfill >> final_backfill
            count_backfill_tasks += 1

        task = el_operator(dag=dags[0], **arg_dict)
        kickoff >> task >> final

    if count_backfill_tasks == 0:
        kickoff_backfill >> final_backfill

    return dags
