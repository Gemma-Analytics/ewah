from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator

from ewah.hooks.base import EWAHBaseHook as BaseHook
from airflow.utils.file import TemporaryDirectory

import os
from copy import deepcopy
from datetime import timedelta
from pymongo import MongoClient
from pymongo import ASCENDING as asc
from tempfile import NamedTemporaryFile

class EWAHMongoDBOperator(EWAHBaseOperator):

    _NAMES = ['mongo', 'mongodb']

    _ACCEPTED_LOAD_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
    }

    _REQUIRES_COLUMNS_DEFINITION = False

    def __init__(self,
        source_collection_name=None, # defaults to target_table_name
        source_database_name=None, # Not required if specified in connection uri
        timestamp_field=None, # required for use with data_from and data_until
        chunking_field=None, # defaults to timestamp_field if None
        chunking_interval=None, # timedelta or integer
        pagination_limit=None, # integer
        single_column_mode=False, # If True, throw all data as json into one col
        ssl=False,
        ssl_conn_id=None,
        mongoclient_extra_args={},
        conn_style='uri', # one of ['uri', 'credentials']
    *args, **kwargs):

        src = source_collection_name or kwargs.get('target_table_name')
        self.source_collection_name = src
        self.source_database_name = source_database_name

        self.timestamp_field = timestamp_field
        self.ssl = ssl or bool(ssl_conn_id)
        self.ssl_conn_id = ssl_conn_id
        self.conn_style = conn_style
        self.mongoclient_extra_args = mongoclient_extra_args

        assert conn_style in ['uri', 'credentials']

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
            if not kwargs.get('load_strategy') == EC.ES_FULL_REFRESH:
                raise Exception('single_column_mode is only compatible with ' \
                    + 'load_strategy = {0}!'.format(EC.ES_FULL_REFRESH))
            if kwargs.get('update_on_columns'):
                raise Exception('single_column_mode is not compatible with ' \
                    + 'update_on_columns!')
            if kwargs.get('primary_key_column_name'):
                raise Exception('single_column_mode is not compatible with ' \
                    + 'primary_key_column_name!')
        self.single_column_mode = single_column_mode

        super().__init__(*args, **kwargs)

        if self.load_data_from or self.load_data_until or self.reload_data_from:
            if not timestamp_field:
                err_msg = 'If using reload_data_from, load_data_from, or load_'
                err_msg += 'data_until, you must also specify timestamp_field!'
                raise Exception(err_msg)

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
            self.upload_data([i for i in collection.find(filter_expressions)])
            return

        # pagination!
        fe = deepcopy(filter_expressions or {})
        if last_id:
            # get next page
            fe.update({self.primary_key_column_name:{'$gt':last_id}})

        data = [i for i in collection.find(fe)
                                    .sort(self.primary_key_column_name, asc) \
                                    .limit(page_size)]

        if not len(data) == 0:
            last_id = data[-1][self.primary_key_column_name]
            self.upload_data(data)
            # call recursively!
            self.extract_and_load_paginated(
                collection=collection,
                filter_expressions=filter_expressions,
                page_size=page_size,
                last_id=last_id,
            )

    def ewah_execute(self, context):
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
        if self.conn_style == 'uri':
            conn_kwargs = {'host': self.source_conn.password} # as uri
        else:
            conn_kwargs = {
                'host': self.source_conn.host,
                'port': self.source_conn.port,
                'tz_aware': True,
            }
            if self.source_conn.login:
                conn_kwargs['username'] = self.source_conn.login
            if self.source_conn.password:
                conn_kwargs['password'] = self.source_conn.password

        with TemporaryDirectory() as tmp_dir:
            db_name = self.source_database_name
            if self.ssl:
                conn_kwargs['tls'] = True
            with NamedTemporaryFile(dir=tmp_dir) as ssl_f:
                with NamedTemporaryFile(dir=tmp_dir) as ssl_f_pk:
                    if self.ssl_conn_id:
                        ssl_f_name = os.path.abspath(ssl_f.name)
                        ssl_f_pk_name = os.path.abspath(ssl_f_pk.name)
                        ssl_conn = BaseHook.get_connection(self.ssl_conn_id)

                        extra = ssl_conn.extra_dejson or ssl_conn.extra
                        if isinstance(extra, dict):
                            # extra is json with certificate & private key
                            certificate_text = extra.get('CERTIFICATE')
                            private_text = extra.get('PRIVATE')
                            ssl_f.write(certificate_text.encode())
                            ssl_f_pk.write(private_text.encode())
                            ssl_f.seek(0)
                            ssl_f_pk.seek(0)
                            conn_kwargs['ssl_certfile'] = ssl_f_name
                            conn_kwargs['ssl_keyfile'] = ssl_f_pk_name
                        else:
                            # extra is text with certificate & private key
                            ssl_f.write(extra.encode())
                            ssl_f.seek(0)
                            conn_kwargs['tlsCertificateKeyFile'] = ssl_f_name
                        if ssl_conn.password:
                            # works for both ways of providing SSL cert
                            conn_kwargs['tlsCertificateKeyFilePassword'] = \
                                ssl_conn.password

                    conn_kwargs.update(self.mongoclient_extra_args)
                    mdb_conn = MongoClient(**conn_kwargs)
                    database = mdb_conn.get_database(name=db_name)

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
