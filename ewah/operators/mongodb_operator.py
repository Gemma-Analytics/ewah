from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import json
from datetime import timedelta
from pymongo import MongoClient
from pymongo import ASCENDING as asc
from bson.json_util import dumps
from copy import deepcopy

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
        pagination_limit=None, # integer
        single_column_mode=False, # If True, throw all data as json into one col
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

        if pagination_limit:
            if not type(pagination_limit) == int:
                raise Exception('pagination_limit must be an integer!')
            if not kwargs.get('primary_key_column_name'):
                raise Exception('pagination_limit must be used with ' \
                    + 'primary_key_column_name!')

        self.pagination_limit = pagination_limit

        if single_column_mode:
            if kwargs.get('columns_definition'):
                raise Exception('single_column_mode is not compatible with '\
                    + 'columns_definition!')
        self.single_column_mode = single_column_mode

        super().__init__(*args, **kwargs)

    def upload_data(self, data):
        if self.single_column_mode:
            # load all data into a single column called 'document'
            data = [{'document':x} for x in data]
            super().upload_data(data)
        else:
            # normal data loading mode
            super().upload_data(data)

    def extract_and_load_paginated(self,
        collection,
        filter_expressions=None,
        page_size=None,
        last_id=None,
    ):
        # adapted from https://www.codementor.io/@arpitbhayani/fast-and-efficient-pagination-in-mongodb-9095flbqr
        if page_size is None:
            # just get all data at once without pagination
            self.upload_data(
                json.loads(dumps(collection.find(filter_expressions)))
            )
            return

        # pagination!
        fe = deepcopy(filter_expressions)
        if last_id:
            # get next page
            fe.update({self.primary_key_column_name:{'$gt':last_id}})
        next_id_data = [x for x in collection.find(fe) \
                        .sort(self.primary_key_column_name, asc) \
                        .skip(page_size - 1)
                        .limit(1)
        ]
        data = json.loads(dumps(collection.find(fe)
                                    .sort(self.primary_key_column_name, asc) \
                                    .limit(page_size)
        ))

        if not len(data) == 0:
            self.upload_data(data)
            if not len(next_id_data) == 0:
                last_id = next_id_data[0][self.primary_key_column_name]
                # call recursively!
                self.extract_and_load_paginated(
                    collection=collection,
                    filter_expressions=filter_expressions,
                    page_size=page_size,
                    last_id=last_id,
                )

    def execute(self, context):
        if not self.drop_and_replace and not self.test_if_target_table_exists():
            self.data_from = self.reload_data_from
            self.log.info('Reloading data from {0}'.format(str(self.data_from)))
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
        collection = database[self.source_collection_name]
        if self.chunking_interval:
            # get data in chunks
            if base_filters:
                match_filters = {}
                for filter in base_filters:
                    for key, value in filter.items():
                        if match_filters.get(key):
                            match_filters[key].update(value)
                        else:
                            match_filters[key] = value
                aggregation_statement = [
                    {"$match": match_filters},
                    {"$group": {
                        "_id": None,
                        "min": {"$min": "$" + self.chunking_field},
                        "max": {"$max": "$" + self.chunking_field},
                    }},
                ]
            else:
                aggregation_statement = [{
                    "$group": {
                        "_id": None,
                        "min": {"$min": "$" + self.chunking_field},
                        "max": {"$max": "$" + self.chunking_field},
                    },
                }]
            self.log.info('Getting data range using:\n{0}'.format(
                str(aggregation_statement)
            ))
            minmax = list(collection.aggregate(aggregation_statement))
            if len(minmax) == 0:
                self.log.info('There appears to be no relevant data!')
                self.upload_data([]) # Upload nothing!
                return
            if not len(minmax) == 1:
                raise Exception('Unexpected error occurred')
            minmax = minmax[0]
            current_val = minmax['min'] # == min_val
            max_val = minmax['max']
            self.log.info('loading data from {0} to {1}.'.format(
                str(current_val),
                str(max_val),
            ))
            while current_val < max_val:
                next_val = min(max_val, current_val + self.chunking_interval)
                self.log.info('Getting data from {0} to {1}...'.format(
                    str(current_val),
                    str(next_val),
                ))

                filter_expressions = deepcopy(base_filters)
                lt_or_lte = '$lte' if next_val == max_val else '$lt'
                filter_expressions += [{self.chunking_field:{
                    '$gte': current_val,
                    lt_or_lte: next_val,
                }}]

                filter_expressions = {'$and': filter_expressions}
                self.log.info('Filter expression: {0}'.format(
                    str(filter_expressions)
                ))

                self.log.info('Uploading chunk...')
                self.extract_and_load_paginated(
                    collection=collection,
                    filter_expressions=filter_expressions,
                    page_size=self.pagination_limit,
                    last_id=None,
                )

                current_val = next_val
        else:
            # Just get all data in one go
            if base_filters:
                filter_expressions = {'$and': base_filters}
            else:
                filter_expressions = None
            self.log.info(
                'Filter expression: {0}'.format(str(filter_expressions))
            )
            # Upload data
            self.extract_and_load_paginated(
                collection=collection,
                filter_expressions=filter_expressions,
                page_size=self.pagination_limit,
                last_id=None,
            )
