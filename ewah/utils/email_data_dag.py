"""DAG to email files from the DWH to a recipient"""
from airflow import DAG
from airflow.operators.email import EmailOperator

from ewah.dwhooks import get_dwhook

import os
import csv
from tempfile import NamedTemporaryFile
from airflow.utils.file import TemporaryDirectory

from datetime import datetime, timedelta


class EWAHDataMailOperator(EmailOperator):
    def __init__(
        self,
        filename,
        dwh_engine,
        dwh_conn_id,
        table,
        schema,
        database=None,
        *args,
        **kwargs
    ):
        self.filename = filename
        self.dwh_engine = dwh_engine
        self.dwh_conn_id = dwh_conn_id
        self.table = table
        self.schema = schema
        self.database = database
        super().__init__(*args, **kwargs)

    def execute(self, context):
        # get data, save temporarily
        dwhook = get_dwhook(self.dwh_engine)(self.dwh_conn_id)
        sql = dwhook._QUERY_TABLE.format(
            **{
                "database_name": self.database,
                "schema_name": self.schema,
                "table_name": self.table,
            }
        )
        self.log.info("Getting data with SQL:\n\n{0}".format(sql))
        data = dwhook.execute_and_return_result(sql, return_dict=True)
        del dwhook

        with TemporaryDirectory(prefix="senddataasmail") as tmp_dir:
            self.files = [tmp_dir + os.sep + self.filename + ".csv"]
            self.log.info(
                "temporarily writing csv file to {0}".format(
                    self.files[0],
                )
            )
            with open(self.files[0], mode="w") as csv_file:
                csvwriter = csv.DictWriter(
                    csv_file,
                    fieldnames=data[0].keys(),
                    delimiter=",",
                    quotechar='"',
                    quoting=csv.QUOTE_MINIMAL,
                )
                csvwriter.writeheader()
                for _ in range(len(data)):
                    datum = data.pop(0)
                    csvwriter.writerow(datum)
            super().execute(context)


def dbt_dag_email_data(
    dag_name,
    recipients,
    subject,
    html_content,
    dwh_engine,
    dwh_conn_id,
    table,
    schema,
    database=None,
    filename=None,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2019, 1, 1),
    default_args=None,
):

    dag = DAG(
        dag_name,
        schedule_interval=schedule_interval,
        start_date=start_date,
        default_args=default_args,
        catchup=False,
    )
    task = EWAHDataMailOperator(
        dag=dag,
        task_id="send_data_as_email_attachment",
        to=recipients,
        subject=subject,
        html_content=html_content,
        filename=filename or dag_name,
        dwh_engine=dwh_engine,
        dwh_conn_id=dwh_conn_id,
        table=table,
        schema=schema,
        database=database,
    )

    return dag
