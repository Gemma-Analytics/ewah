from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from google.cloud import bigquery
from tempfile import NamedTemporaryFile
from datetime import timedelta

import os

class EWAHBigQueryOperator(EWAHSQLBaseOperator):

    _SQL_BASE = '''
        SELECT
            {columns}
        FROM `{database}`.`{schema}`.`{table}`
        WHERE {where_clause}
    '''
    _SQL_BASE_SELECT = \
        'SELECT * FROM ({select_sql}) t WHERE {{0}}'
    _SQL_COLUMN_QUOTE = '`'
    _SQL_MINMAX_CHUNKS = '''
        SELECT MIN(`{column}`), MAX(`{column}`) FROM ({base}) t
    '''
    _SQL_CHUNKING_CLAUSE = '''
        AND {column} >= @from_value
        AND {column} <{equal_sign} @until_value
    '''
    _SQL_PARAMS = '@{0}'

    def __init__(self, *args, is_sharded=False, sharding_column=None, **kwargs):
        self.sql_engine = self._BQ

        # Special case: getting a sharded table; then source_table_name shall be
        # the base name, e.g. events_ if getting events_*
        # To be refactored in the future!
        if is_sharded:
            if kwargs.get('drop_and_replace', True):
                self.is_sharded = False
                kwargs['source_table_name'] += '*' # get all at full refresh!
            else:
                self.is_sharded = True
                self.sharding_column = sharding_column
                self.bq_table_name = kwargs['source_table_name']
                self.bq_schema_name = kwargs['source_schema_name']
                self.bq_dataset_name = kwargs['source_database_name']
        else:
            self.is_sharded = False


        _msg = "Must supply source_database_name!"
        assert kwargs.get('source_database_name'), _msg
        super().__init__(*args, **kwargs)

        if self.is_sharded:
            _msg = "Sharded tables must be an incremental load!"
            assert not self.drop_and_replace, _msg

    def _get_data_from_sql(self, sql, params=None, return_dict=True):

        # set params
        if params:
            type_mapping = EC.QBC_TYPE_MAPPING.get(EC.DWH_ENGINE_BIGQUERY)
            _default = type_mapping[EC.QBC_TYPE_MAPPING_DEFAULT]
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        key,
                        type_mapping.get(type(value), _default),
                        value,
                    )
                    for key, value in params.items()
                ]
            )
        else:
            job_config = None

        # create temp file with creds
        self.log.info('Executing:\n{0}\n\nWith params:\n{1}'.format(
            sql,
            str(params),
        ))
        with NamedTemporaryFile() as cred_json:
            cred_filename = os.path.abspath(cred_json.name)
            cred_json.seek(0)
            cred_json.write(self.source_conn.extra.encode())
            cred_json.seek(0)
            cred_json.flush()
            client = bigquery.Client.from_service_account_json(cred_filename)
            results = client.query(sql, job_config=job_config).result()
            dict_result = [dict(row) for row in results]

        if return_dict or not dict_result:
            return dict_result
        keys = dict_result[0].keys()
        return [[row[key] for key in keys] for row in dict_result]

    def ewah_execute(self, context):
        # If getting a sharded events table, use a different approach

        if not self.is_sharded:
            # This should also run in the case of the full refresh DAG of a
            # fullcremental strategy even if it is a sharded table
            return super().ewah_execute(context)

        # code below only runs if is_sharded
        if self.test_if_target_table_exists():
            date_from = self.date_from or context['execution_date']
            # date_from -= timedelta(days=1) # get data from previous day as well
        else:
            date_from = self.reload_data_from or context['start_date']
        date_until = self.date_until or context['execution_date']
        # don't use next_execution_date: just run this once a day incrementally
        # and get the previous day's data, otherwise it might lead to duplicates

        date_from = airflow_datetime_adjustments(date_from)
        date_until = airflow_datetime_adjustments(date_until)

        while date_from <= date_until:
            self.upload_data(data=self._get_data_from_sql(
                sql=self._SQL_BASE.format(
                    columns='*',
                    database=self.bq_dataset_name,
                    schema=self.bq_schema_name,
                    table=self.bq_table_name + date_from.strftime('%Y%m%d'),
                    where_clause='1=1',
                ),
                return_dict=True,
            ))

            date_from += timedelta(days=1)
