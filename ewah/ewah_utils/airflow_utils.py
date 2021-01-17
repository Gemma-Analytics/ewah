from airflow.operators.python import PythonOperator as PO
from airflow.operators.dummy import DummyOperator as DO
from airflow.models import BaseOperator
from airflow.sensors.sql import SqlSensor

from ewah.hooks.base import EWAHBaseHook
from ewah.constants import EWAHConstants as EC

from datetime import datetime, timedelta, timezone
from copy import deepcopy
import pytz
import re


class EWAHSqlSensor(SqlSensor):
    """Overwrite native SQL sensor to allow usage of ewah custom connection types."""

    def _get_hook(self):
        conn = EWAHBaseHook.get_connection(conn_id=self.conn_id)
        if not conn.conn_type.startswith("ewah"):
            raise Exception("Must use an appropriate EWAH custom connection type!")
        return conn.get_hook()


class PGO(BaseOperator):
    """Airflow operator to execute PostgreSQL statements.

    Cannot use airflow.providers.postgres.operators.postgres.PostgresOperator
        due to the a requirement to have an SSH tunnel option.

    Needs to be otherwise interchangeable with Airflow's PostgresOperator.
    """

    template_fields = ("sql",)
    template_ext = (".sql",)

    def __init__(self, sql, postgres_conn_id, parameters=None, *args, **kwargs):
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.parameters = parameters
        super().__init__(*args, **kwargs)

    def execute(self, context):
        hook = EWAHBaseHook.get_hook_from_conn_id(self.postgres_conn_id)
        hook.execute(sql=self.sql, params=self.parameters, commit=True)
        hook.close()  # SSH tunnel does not close if hook is not closed first


def datetime_utcnow_with_tz():
    return datetime.utcnow().replace(tzinfo=pytz.utc)


def airflow_datetime_adjustments(datetime_raw):
    if type(datetime_raw) == str:
        datetime_string = datetime_raw
        if "-" in datetime_string[10:]:
            tz_sign = "-"
        else:
            tz_sign = "+"
        datetime_strings = datetime_string.split(tz_sign)

        if "T" in datetime_string:
            format_string = "%Y-%m-%dT%H:%M:%S"
        else:
            format_string = "%Y-%m-%d %H:%M:%S"

        if "." in datetime_string:
            format_string += ".%f"

        if len(datetime_strings) == 2:
            if ":" in datetime_strings[1]:
                datetime_strings[1] = datetime_strings[1].replace(":", "")
            datetime_string = tz_sign.join(datetime_strings)
            format_string += "%z"
        elif "Z" in datetime_string:
            format_string += "Z"
        datetime_raw = datetime.strptime(
            datetime_string,
            format_string,
        )
    elif not (datetime_raw is None or isinstance(datetime_raw, datetime)):
        raise Exception(
            "Invalid datetime type supplied! Supply either string"
            + " or datetime.datetime! supplied: {0}".format(str(type(datetime_raw)))
        )

    if datetime_raw and datetime_raw.tzinfo is None:
        datetime_raw = datetime_raw.replace(tzinfo=timezone.utc)

    return datetime_raw


def etl_schema_tasks(
    dag,
    dwh_engine,
    dwh_conn_id,
    target_schema_name,
    target_schema_suffix="_next",
    target_database_name=None,
    read_right_users=None,  # Only for PostgreSQL
    **additional_task_args
):

    if dwh_engine == EC.DWH_ENGINE_POSTGRES:
        sql_kickoff = """
            DROP SCHEMA IF EXISTS "{schema_name}{schema_suffix}" CASCADE;
            CREATE SCHEMA "{schema_name}{schema_suffix}";
        """.format(
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )
        sql_final = """
            DROP SCHEMA IF EXISTS "{schema_name}" CASCADE;
            ALTER SCHEMA "{schema_name}{schema_suffix}"
                RENAME TO "{schema_name}";
        """.format(
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )

        # Don't fail final task just because a user or role that should
        # be granted read rights does not exist!
        grant_rights_sql = """
            DO $$
            BEGIN
              GRANT USAGE ON SCHEMA "{target_schema_name}" TO {user};
              GRANT SELECT ON ALL TABLES
                IN SCHEMA "{target_schema_name}" TO {user};
              EXCEPTION WHEN OTHERS THEN -- catches any error
                RAISE NOTICE 'not granting rights - user does not exist!';
            END
            $$;
        """
        if read_right_users:
            if not isinstance(read_right_users, list):
                raise Exception("Arg read_right_users must be of type List!")
            for user in read_right_users:
                if re.search(r"\s", user) or (";" in user):
                    _msg = "No whitespace or semicolons allowed in usernames!"
                    raise ValueError(_msg)
                sql_final += grant_rights_sql.format(
                    target_schema_name=target_schema_name,
                    user=user,
                )

        task_1_args = deepcopy(additional_task_args)
        task_2_args = deepcopy(additional_task_args)
        task_1_args.update(
            {
                "sql": sql_kickoff,
                "task_id": "kickoff_{0}".format(target_schema_name),
                "dag": dag,
                "postgres_conn_id": dwh_conn_id,
            }
        )
        task_2_args.update(
            {
                "sql": sql_final,
                "task_id": "final_{0}".format(target_schema_name),
                "dag": dag,
                "postgres_conn_id": dwh_conn_id,
            }
        )
        return (PGO(**task_1_args), PGO(**task_2_args))
    elif dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
        target_database_name = target_database_name or (
            EWAHBaseHook.get_connection(dwh_conn_id).database
        )
        sql_kickoff = """
            DROP SCHEMA IF EXISTS
                "{database}"."{schema_name}{schema_suffix}" CASCADE;
            CREATE SCHEMA "{database}"."{schema_name}{schema_suffix}";
        """.format(
            database=target_database_name,
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )
        sql_final = """
            DROP SCHEMA IF EXISTS "{database}"."{schema_name}" CASCADE;
            ALTER SCHEMA "{database}"."{schema_name}{schema_suffix}"
                RENAME TO "{schema_name}";
        """.format(
            database=target_database_name,
            schema_name=target_schema_name,
            schema_suffix=target_schema_suffix,
        )

        def execute_snowflake(sql, conn_id, **kwargs):
            hook = EWAHBaseHook.get_hook_from_conn_id(conn_id)
            hook.execute(sql)
            hook.close()

        task_1_args = deepcopy(additional_task_args)
        task_2_args = deepcopy(additional_task_args)
        task_1_args.update(
            {
                "task_id": "kickoff_{0}".format(target_schema_name),
                "python_callable": execute_snowflake,
                "op_kwargs": {
                    "sql": sql_kickoff,
                    "conn_id": dwh_conn_id,
                },
                "provide_context": True,
                "dag": dag,
            }
        )
        task_2_args.update(
            {
                "task_id": "final_{0}".format(target_schema_name),
                "python_callable": execute_snowflake,
                "op_kwargs": {
                    "sql": sql_final,
                    "conn_id": dwh_conn_id,
                },
                "provide_context": True,
                "dag": dag,
            }
        )
        return (PO(**task_1_args), PO(**task_2_args))
    elif dwh_engine == EC.DWH_ENGINE_GS:
        # create dummy tasks
        return (
            DO(
                task_id="kickoff",
                dag=dag,
            ),
            DO(
                task_id="final",
                dag=dag,
            ),
        )
    else:
        raise ValueError("Feature not implemented!")
