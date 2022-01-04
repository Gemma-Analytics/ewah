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
