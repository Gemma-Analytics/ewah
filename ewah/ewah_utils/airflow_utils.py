from airflow.operators.postgres_operator import PostgresOperator as PGO
from airflow.operators.python_operator import PythonOperator as PO
from airflow.hooks.base_hook import BaseHook

from ewah.dwhooks.dwhook_snowflake import EWAHDWHookSnowflake
from ewah.constants import EWAHConstants as EC

from datetime import datetime, timedelta, timezone
from copy import deepcopy
import re

def airflow_datetime_adjustments(datetime_raw):
    if type(datetime_raw) == str:
        datetime_string = datetime_raw
        if '-' in datetime_string[10:]:
            tz_sign = '-'
        else:
            tz_sign = '+'
        datetime_strings = datetime_string.split(tz_sign)

        if 'T' in datetime_string:
            format_string = '%Y-%m-%dT%H:%M:%S'
        else:
            format_string = '%Y-%m-%d %H:%M:%S'

        if '.' in datetime_string:
            format_string += '.%f'

        if len(datetime_strings) == 2:
            if ':' in datetime_strings[1]:
                datetime_strings[1] = datetime_strings[1].replace(':', '')
            datetime_string = tz_sign.join(datetime_strings)
            format_string += '%z'
        elif 'Z' in datetime_string:
            format_string += 'Z'
        datetime_raw = datetime.strptime(
            datetime_string,
            format_string,
        )
    elif not (type(datetime_raw) in (type(None), datetime)):
        raise Exception('Invalid datetime type supplied! Supply either string' \
            + ' or datetime.datetime! supplied: {0}'.format(
                str(type(datetime_raw))
            ))

    if datetime_raw and datetime_raw.tzinfo is None:
        datetime_raw = datetime_raw.replace(tzinfo=timezone.utc)

    return datetime_raw

def etl_schema_tasks(
        dag,
        dwh_engine,
        dwh_conn_id,
        target_schema_name,
        target_schema_suffix='_next',
        target_database_name=None,
        copy_schema=False,
        read_right_users=None, # Only for PostgreSQL
        **additional_task_args
    ):

    if dwh_engine == EC.DWH_ENGINE_POSTGRES:
        if copy_schema:
            sql_kickoff = '''
                /*
                 * Clone schema function found here: https://wiki.postgresql.org/wiki/Clone_schema
                 * Accessed 9 March 2019
                 * The function is created/replaced in this query to make sure it is
                 * available when called thereafter.
                 * Added an INSERT statement to copy data into the new schema as well.
                 */
                CREATE SCHEMA IF NOT EXISTS "{schema_name}" /* for edge case where schema is created for the first time! */;
                CREATE OR REPLACE FUNCTION "{schema_name}".clone_schema(source_schema text, dest_schema text) RETURNS void AS
                $$

                DECLARE
                  object text;
                  buffer text;
                  default_ text;
                  column_ text;
                BEGIN
                  EXECUTE 'CREATE SCHEMA "' || dest_schema || '"';

                  -- TODO: Find a way to make this sequence's owner is the correct table.
                  FOR object IN
                    SELECT sequence_name::text FROM information_schema.SEQUENCES WHERE sequence_schema = source_schema
                  LOOP
                    EXECUTE 'CREATE SEQUENCE "' || dest_schema || '"."' || object || '"';
                  END LOOP;

                  FOR object IN
                    SELECT TABLE_NAME::text FROM information_schema.TABLES WHERE table_schema = source_schema
                  LOOP
                    buffer := '"' || dest_schema || '"."' || object || '"';
                    EXECUTE 'CREATE TABLE ' || buffer || ' (LIKE "' || source_schema || '"."' || object || '" INCLUDING CONSTRAINTS INCLUDING INDEXES INCLUDING DEFAULTS)';

                    FOR column_, default_ IN
                      SELECT column_name::text, REPLACE(column_default::text, source_schema, dest_schema) FROM information_schema.COLUMNS WHERE table_schema = dest_schema AND TABLE_NAME = object AND column_default LIKE 'nextval(%' || source_schema || '%::regclass)'
                    LOOP
                      EXECUTE 'ALTER TABLE ' || buffer || ' ALTER COLUMN "' || column_ || '" SET DEFAULT ' || default_;
                    END LOOP;
                    EXECUTE 'INSERT INTO ' || buffer || ' SELECT * FROM "' || source_schema || '"."' || object || '"';
                  END LOOP;

                END;

                $$ LANGUAGE plpgsql VOLATILE;

                /*
                 * Clone the schema
                 */
                DROP SCHEMA IF EXISTS "{schema_name}{schema_suffix}" CASCADE;
                SELECT "{schema_name}".clone_schema('{schema_name}', '{schema_name}{schema_suffix}');
                '''.format(**{
                    'schema_name': target_schema_name,
                    'schema_suffix': target_schema_suffix,
                })
        else:
            sql_kickoff = ''' /* Create a new, empty schema */
                DROP SCHEMA IF EXISTS "{schema_name}{schema_suffix}" CASCADE;
                CREATE SCHEMA "{schema_name}{schema_suffix}";
                '''.format(**{
                    'schema_name': target_schema_name,
                    'schema_suffix': target_schema_suffix,
                })
        sql_final = '''
            DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;
            ALTER SCHEMA "{schema_name}{schema_suffix}" RENAME TO "{schema_name}";
            '''.format(**{
                'schema_name': target_schema_name,
                'schema_suffix': target_schema_suffix,
            })

        if read_right_users:
            if not type(read_right_users) == list:
                raise Exception('Arg read_right_users must be of type List!')
            for user in read_right_users:
                if re.search(r"\s", user) or (';' in user):
                    raise ValueError('No whitespace or semicolons allowed in usernames!')
                sql_final += f'\nGRANT USAGE ON SCHEMA "{target_schema_name}" TO {user};'
                sql_final += f'\nGRANT SELECT ON ALL TABLES IN SCHEMA "{target_schema_name}" TO {user};'

        task_1_args = deepcopy(additional_task_args)
        task_2_args = deepcopy(additional_task_args)
        task_1_args.update({
            'sql': sql_kickoff,
            'task_id': 'kickoff_{0}'.format(target_schema_name),
            'dag': dag,
            'postgres_conn_id': dwh_conn_id,
            # 'retries': 1,
            # 'retry_delay': timedelta(minutes=1),
        })
        task_2_args.update({
            'sql': sql_final,
            'task_id': 'final_{0}'.format(target_schema_name),
            'dag': dag,
            'postgres_conn_id': dwh_conn_id,
            # 'retries': 1,
            # 'retry_delay': timedelta(minutes=1),
        })
        return (PGO(**task_1_args), PGO(**task_2_args))
    elif dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
        target_database_name = target_database_name or (BaseHook \
                .get_connection(dwh_conn_id).extra_dejson.get('database'))
        if copy_schema:
            sql_kickoff = '''
                /*Make sure there is something to clone in step 2*/
                DROP SCHEMA IF EXISTS "{database}"."{schema_name}{schema_suffix}" CASCADE;
                CREATE SCHEMA IF NOT EXISTS "{database}"."{schema_name}";
                CREATE SCHEMA "{database}"."{schema_name}{schema_suffix}"
                CLONE "{database}"."{schema_name}";
                '''.format(**{
                    'database': target_database_name,
                    'schema_name': target_schema_name,
                    'schema_suffix': target_schema_suffix,
                })
        else:
            sql_kickoff = '''
                DROP SCHEMA IF EXISTS "{database}"."{schema_name}{schema_suffix}" CASCADE;
                CREATE SCHEMA "{database}"."{schema_name}{schema_suffix}";
                '''.format(**{
                    'database': target_database_name,
                    'schema_name': target_schema_name,
                    'schema_suffix': target_schema_suffix,
                })
        sql_final = '''
            DROP SCHEMA IF EXISTS "{database}"."{schema_name}" CASCADE;
            ALTER SCHEMA "{database}"."{schema_name}{schema_suffix}" RENAME TO "{schema_name}";
            '''.format(**{
                'database': target_database_name,
                'schema_name': target_schema_name,
                'schema_suffix': target_schema_suffix,
            })

        def execute_snowflake(sql, conn_id, **kwargs):
            hook = EWAHDWHookSnowflake(conn_id)
            hook.execute(sql)
            hook.close()

        task_1_args = deepcopy(additional_task_args)
        task_2_args = deepcopy(additional_task_args)
        task_1_args.update({
            'task_id': 'kickoff_{0}'.format(target_schema_name),
            'python_callable': execute_snowflake,
            'op_kwargs': {
                'sql': sql_kickoff,
                'conn_id': dwh_conn_id,
            },
            'provide_context': True,
            'dag': dag,
        })
        task_2_args.update({
            'task_id': 'final_{0}'.format(target_schema_name),
            'python_callable': execute_snowflake,
            'op_kwargs': {
                'sql': sql_final,
                'conn_id': dwh_conn_id,
            },
            'provide_context': True,
            'dag': dag,
        })
        return (PO(**task_1_args), PO(**task_2_args))
    else:
        raise ValueError('Feature not implemented!')
