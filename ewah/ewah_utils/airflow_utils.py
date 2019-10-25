from airflow.operators.postgres_operator import PostgresOperator as PGO
from airflow.operators.python_operator import PythonOperator as PO

from ewah.dwhooks.dwhook_snowflake import EWAHDWHookSnowflake
from ewah.constants import EWAHConstants as EC

from datetime import datetime, timedelta
import re

def datetime_from_string(datetime_string):
    try:
        return datetime.strptime(
            datetime_string.split('+')[0].split('.')[0],
            '%Y-%m-%dT%H:%M:%S',
        )
    except ValueError:
        return datetime.strptime(
            datetime_string.split('+')[0].split('.')[0],
            '%Y-%m-%d %H:%M:%S',
        )

def etl_schema_tasks(
        dag,
        dwh_engine,
        dwh_conn_id,
        target_schema_name,
        target_schema_suffix='_next',
        target_database_name=None,
        copy_schema=False,
        read_right_users=None, # Only for PostgreSQL
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

        return (
                PGO(
                    sql=sql_kickoff,
                    task_id='kickoff_'+target_schema_name,
                    dag=dag,
                    postgres_conn_id=dwh_conn_id,
                    retries=1,
                    retry_delay=timedelta(minutes=1),
                ),
                PGO(
                    sql=sql_final,
                    task_id='final_'+target_schema_name,
                    dag=dag,
                    postgres_conn_id=dwh_conn_id,
                    retries=1,
                    retry_delay=timedelta(minutes=1),
                ),
        )
    elif dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
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

        return (
            PO(
                task_id='kickoff_'+target_schema_name,
                python_callable=execute_snowflake,
                op_kwargs={
                    'sql': sql_kickoff,
                    'conn_id': dwh_conn_id,
                },
                provide_context=True,
                dag=dag,
            ),
            PO(
                task_id='final_'+target_schema_name,
                python_callable=execute_snowflake,
                op_kwargs={
                    'sql': sql_final,
                    'conn_id': dwh_conn_id,
                },
                provide_context=True,
                dag=dag,
            ),
        )
    else:
        raise ValueError('Feature not implemented!')
