from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import datetime_from_string
from ewah.constants import EWAHConstants as EC

from datetime import timedelta

class EWAHSQLBaseOperator(EWAHBaseOperator):

    template_fields = ('data_from', 'data_until')

    # implemented SQL sources - set self.sql_engine to this value in operator
    _MYSQL = 'MySQL'
    _PGSQL = 'PostgreSQL'

    def __init__(self,
        source_conn_id, # string
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
            if not (data_from and data_until):
                raise Exception('If one of data_from and data_until is ' \
                    + 'specified, the other has to be specified as well!')

        if chunking_interval:
            if type(chunking_interval) == timedelta:
                if not timestamp_column:
                    raise Exception("If chunking via timedelta, must supply" \
                        + " a timestamp_column, even if not loading " \
                        + "incrementally!")
            elif type(chunking_interval) == int:
                if not kwargs.get('primary_key_column_name'):
                    raise Exception("If chunking via integer, must supply" \
                        + " primary_key_column_name!")
            else:
                raise Exception("Arg chunking_interval must be integer or "\
                    + "datetime.timedelta!")

        super().__init__(*args, **kwargs)

        source_table_name = source_table_name or self.target_table_name

        if not self.drop_and_replace:
            if not (data_from and data_until):
                raise Exception('For incremental loading, you must specify ' \
                    + 'both data_from and data_until!')

        self.source_conn_id = source_conn_id
        self.source_schema_name = source_schema_name
        self.source_table_name = source_table_name
        self.data_from = data_from
        self.data_until = data_until
        self.timestamp_column = timestamp_column
        self.chunking_interval = chunking_interval
        self.reload_data_from = reload_data_from
        self.reload_data_chunking = reload_data_chunking or chunking_interval

    def execute(self, context):

        if self.drop_and_replace:
            self.log.info('Loading data as full refresh.')
        else:
            if type(self.data_from) == str:
                self.data_from = datetime_from_string(self.data_from)
            if type(self.data_until) == str:
                self.data_until = datetime_from_string(self.data_until)

            if not self.test_if_target_table_exists():
                self.chunking_interval = self.reload_data_chunking \
                    or self.chunking_interval \
                    or (self.data_from - self.data_until)
                self.data_from = self.reload_data_from \
                    or context['dag'].start_date
                if type(self.data_from) == str:
                    self.data_from = datetime_from_string(self.data_from)

            str_format = '%Y-%m-%dT%H:%M:%SZ'
            self.log.info('Incrementally loading data from {0} to {1}.'.format(
                self.data_from.strftime(str_format),
                self.data_until.strftime(str_format),
            ))


        if self.columns_definition:
            sql_base = self._SQL_BASE_COLUMNS.format(
                '{0}, {0}'.format(self._SQL_COLUMN_QUOTE)
                    .join(self.columns_definition.keys()),
                self.source_schema_name,
                self.source_table_name,
            )
        else:
            sql_base = self._SQL_BASE_ALL.format(
                self.source_schema_name,
                self.source_table_name,
            )

        if self.drop_and_replace:
            sql_base = sql_base.format('TRUE {0}')
        else:
            sql_base = sql_base.format('{3}{0}{3}>={1} AND {3}{0}{3}<{2} {{0}}'
                .format(
                    self.timestamp_column,
                    self.data_from.strftime(str_format),
                    self.data_until.strftime(str_format),
                    self._SQL_COLUMN_QUOTE,
            ))


        if self.chunking_interval:
            if type(self.chunking_interval) == timedelta:
                chunking_column = self.timestamp_column
            else:
                chunking_column = self.primary_key_column_name

            if self.drop_and_replace:
                previous_chunk, max_chunk=self._get_data_from_sql(
                    sql=self._SQL_MINMAX_CHUNKS.format(
                        chunking_column,
                        self.source_schema_name,
                        self.source_table_name,
                    ),
                    return_dict=False,
                )[0]
            else:
                previous_chunk = self.data_from
                max_chunk = self.until

            while previous_chunk <= max_chunk:
                data = self._get_data_from_sql(
                    sql=sql_base.format(
                        self._SQL_CHUNKING_CLAUSE
                    ).format(
                        chunking_column,
                        '=' if max_chunk < (previous_chunk \
                            + self.chunking_interval) else '',
                    ),
                    params={
                        'from': previous_chunk,
                        'until': min(
                            max_chunk,
                            previous_chunk + self.chunking_interval,
                        ),
                    },
                    return_dict=True,
                )

                self.upload_data(
                    data=data,
                )
                previous_chunk += self.chunking_interval
        else:
            self.upload_data(
                data=self._get_data_from_sql(
                    sql=sql_base.format('AND TRUE'),
                    return_dict=True,
                ),
            )
