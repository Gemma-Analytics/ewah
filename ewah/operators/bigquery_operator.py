from ewah.operators.sql_base_operator import EWAHSQLBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from google.cloud import bigquery
from tempfile import NamedTemporaryFile

import os

class EWAHBigQueryOperator(EWAHSQLBaseOperator):

    _SQL_BASE = \
        'SELECT\n{columns}\nFROM `{schema}`.`{table}`\nWHERE {where_clause}'
    _SQL_BASE_SELECT = \
        'SELECT * FROM ({select_sql}) t WHERE {{0}}'
    _SQL_COLUMN_QUOTE = '`'
    _SQL_MINMAX_CHUNKS = 'SELECT MIN({column}), MAX({column}) FROM ({base}) t;'
    _SQL_CHUNKING_CLAUSE = '''
        AND {column} >= @from_value
        AND {column} <{equal_sign} @until_value
    '''
    _SQL_PARAMS = '@{0}'

    def __init__(self, *args, **kwargs):
        self.sql_engine = self._BQ
        super().__init__(*args, **kwargs)

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
