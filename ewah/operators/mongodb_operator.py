from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import json
from datetime import timedelta
from pymongo import MongoClient
from bson.json_util import dumps

class EWAHMongoDBOperator(EWAHBaseOperator):

    template_fields = ('data_from', 'data_until', 'reload_data_from')

    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = True

    def __init__(self,
        source_collection_name=None, # defaults to target_table_name
        source_database_name=None, # Not required if specified in connection uri
        data_from=None, # string (can be templated) or datetime
        data_until=None, # as data_from
        timestamp_field=None, # required for use with data_from and data_until
        chunking_field=None, # defaults to timestamp_field if None
        chunking_interval=None, # timedelta or integer
        reload_data=None, # Boolean - Reload data if table is missing?
        reload_data_from=None, # string (can be templated), datetime or None
        reload_data_chunking=None, # defaults to chunking_interval
    *args, **kwargs):

        source_collection_name = source_collection_name \
                                    or kwargs.get('target_table_name')

        chunking_field = chunking_field or timestamp_field

    def get_data(self, uri, filter_expression=None):
        if self.source_database_name:
            database = MongoClient(uri).get_database(self.source_database_name)
        else:
            database = MongoClient(uri).get_database()
        data = database[self.source_collection_name].find(filter_expression)
        return json.loads(dumps(data))

    def execute(self, context):
        # airflow_datetime_adjustments
        uri = BaseHook.get_connection(self.source_conn_id).password
