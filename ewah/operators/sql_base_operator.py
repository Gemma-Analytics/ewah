from ewah.operators.base_operator import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from sshtunnel import SSHTunnelForwarder
from tempfile import NamedTemporaryFile
from datetime import timedelta
from pytz import timezone

import os

class EWAHSQLBaseOperator(EWAHBaseOperator):

    # implemented SQL sources - set self.sql_engine to this value in operator
    _MYSQL = 'MySQL'
    _PGSQL = 'PostgreSQL'
    _ORACLE = 'OracleSQL'
    _BQ = 'BigQuery'

    _ACCEPTED_SOURCES = [
        _MYSQL,
        _PGSQL,
        _ORACLE,
        _BQ,
    ]

    _ACCEPTED_LOAD_STRATEGIES = {
        EC.LS_FULL_REFRESH: True,
        EC.LS_INCREMENTAL: True,
        EC.LS_APPENDING: False,
    }

    def __init__(self,
        source_schema_name=None, # string
        source_table_name=None, # string, defaults to same as target_table_name
        source_database_name=None, # bigquery: project id
        sql_select_statement=None, # SQL as alternative to source_table_name
        use_execution_date_for_incremental_loading=False, # use context instead
        #   of data_from and data_until
        timestamp_column=None, # name of column to increment and/or chunk by
        chunking_interval=None, # can be datetime.timedelta or integer
        chunking_column=None, # defaults to primary key if integer
        # also potentially used: primary_key_column_name of parent operator
        where_clause=None, # restrict data loaded by operator
    *args, **kwargs):
        where_clause = where_clause or '1 = 1'

        # allow setting schema in general config w/out throwing an error
        if sql_select_statement:
            source_schema_name = None
            source_database_name = None

        target_table_name = kwargs.get('target_table_name')

        if not hasattr(self, 'sql_engine'):
            raise Exception('Operator invalid: need attribute sql_engine!')
        if not self.sql_engine in self._ACCEPTED_SOURCES:
            raise Exception('Operator invalid: SQL engine not implemented!')

        if chunking_interval:
            if type(chunking_interval) == timedelta:
                if not timestamp_column:
                    raise Exception("If chunking via timedelta, must supply" \
                        + " a timestamp_column, even if not loading " \
                        + "incrementally!")
            elif type(chunking_interval) == int:
                if not chunking_column \
                    and not kwargs.get('primary_key_column_name'):
                    # Check columns for primary key - if exactly one,
                    # use it. Otherwise, raise error.
                    error_msg = "If chunking via integer, must supply " \
                        + "primary_key_column_name OR have EXACTLY ONE " \
                        + "primary key defined in the columns_definition dict" \
                        + "! This is not the case for: {0}. {{0}}" \
                            .format(source_table_name)
                    if not kwargs.get('columns_definition'):
                        raise Exception(error_msg.format(
                            'You did not supply the columns_definition dict.'
                        ))
                    columns = kwargs.get('columns_definition')
                    if not (sum([
                            1 if columns[key].get(EC.QBC_FIELD_PK) else 0
                            for key in columns.keys()
                        ]) == 1):
                        raise Exception(error_msg.format(
                            'There is not exactly one primary key in the dict.'
                        ))
                    for key in columns.keys():
                        if columns[key].get(EC.QBC_FIELD_PK):
                            kwargs['primary_key_column_name'] = key
                            break
                chunking_column = chunking_column \
                                        or kwargs['primary_key_column_name']

            else:
                raise Exception("Arg chunking_interval must be integer or "\
                    + "datetime.timedelta!")

        self.use_execution_date_for_incremental_loading = \
            use_execution_date_for_incremental_loading
        self.timestamp_column = timestamp_column
        self.chunking_interval = chunking_interval
        self.chunking_column = chunking_column

        if kwargs.get('reload_data_from') and not chunking_interval:
            raise Exception('When setting reload_data_from, must also set ' \
                + 'chunking_interval!')
        
        # run after setting class properties for templating
        super().__init__(*args, **kwargs)

        if self.load_data_from or self.load_data_until or \
            use_execution_date_for_incremental_loading:
            if not timestamp_column:
                raise Exception("If you used data_from and/or data_until, you" \
                    + " must also use timestamp_column to specify the column" \
                    + " that is being used!")

        # self.base_select is a SELECT statement (i.e. a string) ending in a
        #   WHERE {0} -> the extract process can add conditions!
        # self.base_sql is a pure SELECT statement ready to be executed
        if sql_select_statement:
            err_msg = 'sql_select_statement and {0} cannot' \
                ' be used in combination!'
            if not where_clause == '1 = 1':
                raise Exception(err_msg.format('where_clause'))
            if source_table_name:
                raise Exception(err_msg.format('source_table_name'))
            self.base_sql = sql_select_statement
        else:
            source_table_name = source_table_name or target_table_name

            if self.columns_definition:
                self.base_sql = self._SQL_BASE.format(**{
                    'columns': (
                        '\t'
                        + self._SQL_COLUMN_QUOTE
                        + (
                            '{0}\n,\t{0}'
                            .format(self._SQL_COLUMN_QUOTE)
                            .join(self.columns_definition.keys())
                        )
                        + self._SQL_COLUMN_QUOTE
                    ),
                    'database': source_database_name,
                    'schema': source_schema_name,
                    'table': source_table_name,
                    'where_clause': where_clause,
                })
            else:
                self.base_sql = self._SQL_BASE.format(**{
                    'columns': '\t*',
                    'database': source_database_name,
                    'schema': source_schema_name,
                    'table': source_table_name,
                    'where_clause': where_clause,
                })
        self.base_select = self._SQL_BASE_SELECT.format(**{
            'select_sql': self.base_sql,
        })

    def ewah_execute(self, context):
        str_format = '%Y-%m-%dT%H:%M:%SZ'

        if self.load_strategy == EC.LS_FULL_REFRESH:
            self.log.info('Loading data as full refresh.')
            self.data_from = None
            self.data_until = None
        else:
            self.data_from = self.load_data_from
            self.data_until = self.load_data_until
            if not self.test_if_target_table_exists():
                self.chunking_interval = self.chunking_interval \
                    or (self.data_from - self.data_until)

            self.log.info('Incrementally loading data from {0} to {1}.'.format(
                self.data_from.strftime(str_format),
                self.data_until.strftime(str_format),
            ))

        params = {}
        # _SQL_PARAMS
        if self.load_strategy == EC.LS_FULL_REFRESH:
            sql_base = self.base_select
            if self.data_from:
                sql_base = sql_base.format('{0} >= {1} AND {{0}}'.format(
                    self.timestamp_column,
                    self._SQL_PARAMS.format('data_from'),
                ))
                params.update({'data_from': self.data_from})
            if self.data_until:
                sql_base = sql_base.format('{0} <= {1} AND {{0}}'.format(
                    self.timestamp_column,
                    self._SQL_PARAMS.format('data_until'),
                ))
                params.update({'data_until': self.data_from})
            sql_base = sql_base.format('1 = 1 {0}')
        else:
            sql_base = self.base_select.format('{0} >= {1} AND {0} < {2} {{0}}'
                .format(
                    self.timestamp_column,
                    self._SQL_PARAMS.format('data_from'),
                    self._SQL_PARAMS.format('data_until'),
            ))
            params.update({'data_from': self.data_from})
            params.update({'data_until': self.data_until})


        if self.chunking_interval:
            chunking_column = self.chunking_column
            if isinstance(self.chunking_interval, timedelta):
                chunking_column = chunking_column or self.timestamp_column

            if self.load_strategy == EC.LS_FULL_REFRESH:
                previous_chunk, max_chunk = self._get_data_from_sql(
                    sql=self._SQL_MINMAX_CHUNKS.format(**{
                        'column': chunking_column,
                        'base': sql_base.format(''),
                    }),
                    params=params,
                    return_dict=False,
                )[0]
                if chunking_column == self.timestamp_column:
                    tz = timezone('UTC')
                    if not previous_chunk.tzinfo:
                        previous_chunk = tz.localize(previous_chunk)
                    if self.data_from:
                        previous_chunk = max(previous_chunk, self.data_from)
                    if not max_chunk.tzinfo:
                        max_chunk = tz.localize(max_chunk)
                    if self.data_until:
                        max_chunk = min(max_chunk, self.data_until)

                if previous_chunk is None or max_chunk is None:
                    self.log.info('There appears to be no data?')
                    return

            else:
                previous_chunk = self.data_from
                max_chunk = self.data_until

            while previous_chunk <= max_chunk:
                params.update({
                    'from_value': previous_chunk,
                    'until_value': min(
                        max_chunk,
                        previous_chunk + self.chunking_interval,
                    ),
                })
                data = self._get_data_from_sql(
                    sql=sql_base.format(
                        self._SQL_CHUNKING_CLAUSE
                    ).format(**{
                        'column': chunking_column,
                        'equal_sign': ('=' if max_chunk < (previous_chunk \
                            + self.chunking_interval) else ''),
                    }),
                    params=params,
                    return_dict=True,
                )

                self.upload_data(data=data)
                previous_chunk += self.chunking_interval
        else:
            self.upload_data(
                data=self._get_data_from_sql(
                    sql=sql_base.format('AND 1 = 1'),
                    return_dict=True,
                    params=params,
                ),
            )
