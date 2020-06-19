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

    _REQUIRES_COLUMNS_DEFINITION = False

    def __init__(self,
        source_collection_name=None, # defaults to target_table_name
        source_database_name=None, # Not required if specified in connection uri
        data_from=None, # string (can be templated) or datetime
        data_until=None, # as data_from
        timestamp_field=None, # required for use with data_from and data_until
        chunking_field=None, # defaults to timestamp_field if None
        chunking_interval=None, # timedelta or integer
        reload_data_from=None, # string (can be templated), datetime or None
    *args, **kwargs):

        src = source_collection_name or kwargs.get('target_table_name')
        self.source_collection_name = src
        self.source_database_name = source_database_name

        if data_from or data_until or reload_data_from:
            if not timestamp_field:
                err_msg = 'If using reload_data_from, data_from, or data_until, '
                err_msg += 'you must also specify timestamp_field!'
                raise Exception(err_msg)
        self.timestamp_field = timestamp_field
        self.data_from = data_from
        self.data_until = data_until
        self.reload_data_from = reload_data_from

        chunking_field = chunking_field or timestamp_field
        if chunking_interval:
            if not (type(chunking_interval) in [int, timedelta]):
                raise Exception('chunking_interval must be int or timedelta!')
            if not chunking_field:
                raise Exception('if chunking_interval is set, must also set ' \
                    'chunking_field or timestamp_field!')
        self.chunking_interval = chunking_interval
        self.chunking_field = chunking_field

    def execute(self, context):
        if not self.drop_and_replace and not self.test_if_table_exists():
            self.data_from = self.reload_data_from
        self.data_from = airflow_datetime_adjustments(self.data_from)
        self.data_until = airflow_datetime_adjustments(self.data_until)

        self.log.info('Creating filter expression...')
        base_filters = []

        # data_from and data_until filter expression
        if self.timestamp_field:
            if self.data_from:
                base_filters += [{
                    self.timestamp_field: {'$gte': self.data_from}
                }]
            if self.data_until:
                base_filters += [{
                    self.timestamp_field: {'$lt': self.data_until}
                }]

        self.log.info('Connecting to MongoDB Database...')
        uri = BaseHook.get_connection(self.source_conn_id).password
        database = MongoClient(uri).get_database(name=self.source_database_name)

        self.log.info('Getting data...')
        db = database[self.source_collection_name]
        if self.chunking_interval:
            # get data in chunks
            min_val =
            max_val =
            current_val = min_val
            while current_val < max_val:
                next_val = min(max_val, current_val + self.chunking_interval)
                self.log.info('Getting data from {0} to {1}...'.format(
                    str(current_val),
                    str(next_val),
                ))

                filter_expressions = base_filters
                lt_or_lte = 'lte' if next_val = max_val else 'lt'
                filter_expressions += [{self.chunking_field:{
                    '$gte': current_val,
                    lt_or_lte: next_val,
                }}]

                filter_expressions = {'$and': filter_expressions}
                self.log.info('Filter expression: {0}'.format(
                    str(filter_expressions)
                ))
                data = db.find(filter_expressions)
                data = json.loads(dumps(data))

                self.log.info('Uploading chunk...')
                self.upload(data)

                current_val = next_val
        else:
            # Just get all data in one go


            if base_filters:
                filter_expressions = {'$and': base_filters}
            else:
                filter_expressions = None
            data = db.find(filter_expressions)
            data = json.loads(dumps(data))

            # Upload data
            self.upload_data(data)
