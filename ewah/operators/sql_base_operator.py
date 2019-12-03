from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.constants import EWAHConstants as EC

from datetime import timedelta
from pytz import timezone

class EWAHSQLBaseOperator(EWAHBaseOperator):

    template_fields = ('data_from', 'data_until', 'reload_data_from')

    # implemented SQL sources - set self.sql_engine to this value in operator
    _MYSQL = 'MySQL'
    _PGSQL = 'PostgreSQL'
    _ORACLE = 'OracleSQL'

    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = True

    def __init__(self,
        source_schema_name=None, # string
        source_table_name=None, # string, defaults to same as target_table_name
        data_from=None, # datetime, ISO datetime string, or airflow JINJA macro
        data_until=None, # datetime, ISO datetime string, or airflow JINJA macro
        timestamp_column=None, # name of column to increment and/or chunk by
        chunking_interval=None, # can be datetime.timedelta or integer
        # also potentially used: primary_key_column_name of parent operator
        reload_data_from=None, # If a new table is added in production, and
        #   it is loading incrementally, where to start loading data? datetime
        reload_data_chunking=None, # must be timedelta
    *args, **kwargs):

        source_table_name = source_table_name or kwargs.get('target_table_name')

        if not hasattr(self, 'sql_engine'):
            raise Exception('Operator invalid: need attribute sql_engine!')

        if reload_data_from and not (reload_data_chunking or chunking_interval):
            raise Exception('When setting reload_data_from, must also set ' \
                + 'either reload_data_chunking or chunking_interval!')

        if data_from or data_until:
            if not timestamp_column:
                raise Exception("If you used data_from and/or data_until, you" \
                    + " must also use timestamp_column to specify the column" \
                    + " that is being used!")

        if chunking_interval:
            if type(chunking_interval) == timedelta:
                if not timestamp_column:
                    raise Exception("If chunking via timedelta, must supply" \
                        + " a timestamp_column, even if not loading " \
                        + "incrementally!")
            elif type(chunking_interval) == int:
                if not kwargs.get('primary_key_column_name'):
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

            else:
                raise Exception("Arg chunking_interval must be integer or "\
                    + "datetime.timedelta!")

        super().__init__(*args, **kwargs)

        if not self.drop_and_replace:
            if not (data_from and data_until):
                raise Exception('For incremental loading, you must specify ' \
                    + 'both data_from and data_until!')


        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.data_from = data_from
        self.data_until = data_until
        self.timestamp_column = timestamp_column
        self.chunking_interval = chunking_interval
        self.reload_data_from = reload_data_from
        self.reload_data_chunking = reload_data_chunking or chunking_interval

    def execute(self, context):
        str_format = '%Y-%m-%dT%H:%M:%SZ'

        self.data_from = airflow_datetime_adjustments(self.data_from)
        self.data_until = airflow_datetime_adjustments(self.data_until)
        self.reload_data_from = \
            airflow_datetime_adjustments(self.reload_data_from)

        if self.drop_and_replace:
            self.log.info('Loading data as full refresh.')
        else:
            if not self.test_if_target_table_exists():
                self.chunking_interval = self.reload_data_chunking \
                    or self.chunking_interval \
                    or (self.data_from - self.data_until)
                self.data_from = self.reload_data_from \
                    or context['dag'].start_date
                if type(self.data_from) == str:
                    self.data_from = datetime_from_string(self.data_from)

            self.log.info('Incrementally loading data from {0} to {1}.'.format(
                self.data_from.strftime(str_format),
                self.data_until.strftime(str_format),
            ))


        if self.columns_definition:
            sql_base = self._SQL_BASE_COLUMNS.format(**{
                'columns': ('{0}, {0}'.format(self._SQL_COLUMN_QUOTE)
                    .join(self.columns_definition.keys())),
                'schema': self.source_schema_name,
                'table': self.source_table_name,
            })
        else:
            sql_base = self._SQL_BASE_ALL.format(**{
                'schema': self.source_schema_name,
                'table': self.source_table_name,
            })

        params = {}
        # _SQL_PARAMS
        if self.drop_and_replace:
            if self.data_from:
                sql_base = sql_base.format('{0}{1}{0} >= {2} AND {{0}}'.format(
                    self._SQL_COLUMN_QUOTE,
                    self.timestamp_column,
                    self._SQL_PARAMS.format('data_from'),
                ))
                params.update({'data_from': self.data_from})
            if self.data_until:
                sql_base = sql_base.format('{0}{1}{0} <= {2} AND {{0}}'.format(
                    self._SQL_COLUMN_QUOTE,
                    self.timestamp_column,
                    self._SQL_PARAMS.format('data_until'),
                ))
                params.update({'data_until': self.data_from})
            sql_base = sql_base.format('1 = 1 {0}')
        else:
            sql_base = sql_base.format('{3}{0}{3}>={1} AND {3}{0}{3}<{2} {{0}}'
                .format(
                    self.timestamp_column,
                    self._SQL_PARAMS.format('data_from'),
                    self._SQL_PARAMS.format('data_until'),
                    self._SQL_COLUMN_QUOTE,
            ))
            params.update({'data_from': self.data_from})
            params.update({'data_until': self.data_from})


        if self.chunking_interval:
            if type(self.chunking_interval) == timedelta:
                chunking_column = self.timestamp_column
            else:
                chunking_column = self.primary_key_column_name

            if self.drop_and_replace:
                previous_chunk, max_chunk = self._get_data_from_sql(
                    sql=self._SQL_MINMAX_CHUNKS.format(**{
                        'column': chunking_column,
                        'schema': self.source_schema_name,
                        'table': self.source_table_name,
                    }),
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
                ),
            )
