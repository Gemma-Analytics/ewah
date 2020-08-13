from datetime import datetime
from yahoofinancials import YahooFinancials

from ewah.constants import EWAHConstants as EC
from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments

class EWAHFXOperator(EWAHBaseOperator):

    template_fields = ('data_from', 'data_until')

    _IS_INCREMENTAL = False
    _IS_FULL_REFRESH = True

    def __init__(
        self,
        currency_pair, # iterable of length 2
        data_from=None, # data_from defaults to start_date
        data_until=None, # data_until defaults to now
        frequency='daily', # daily, weekly, or monthly
    *args, **kwargs):

        if not type(data_from) in (str, datetime, type(None)):
            raise Exception('If supplied, data_from must be string, datetime' \
                + ', or None!')
        if not type(data_until) in (str, datetime, type(None)):
            raise Exception('If supplied, data_until must be string, datetime' \
                + ', or None!')

        if not frequency in ('daily', 'weekly', 'monthly'):
            raise Exception('Frequency must be one of: daily, weekly, monthly')

        if not len(currency_pair) == 2:
            raise Exception('currency_pair must be iterable of length 2 ' \
                + 'containing the currency pair.')

        # No connection id required for this operator
        if kwargs.get('source_conn_id'):
            self.log.info('source_conn_id is not required for operator! ' \
                + 'Ignoring argument.')
        kwargs['source_conn_id'] = None
        # If incremental, the primary key column is 'date'
        if kwargs.get('update_on_columns'):
            self.log.info('update_on_columns is fixed for this operator. ' \
                + 'Using the default.')
        kwargs['update_on_columns'] = ['date']
        super().__init__(*args, **kwargs)

        self.currency_pair = currency_pair
        self.data_from = data_from
        self.data_until = data_until
        self.frequency = frequency

    def execute(self, context):
        self.data_from = self.data_from or context['dag'].start_date
        self.data_until = self.data_until or datetime.now()

        self.data_from = airflow_datetime_adjustments(self.data_from)
        self.data_until = airflow_datetime_adjustments(self.data_until)

        format_str = '%Y-%m-%d'
        currency_str = '{0}{1}=X'.format(*self.currency_pair)
        data = YahooFinancials([currency_str]).get_historical_price_data(
            self.data_from.strftime(format_str),
            self.data_until.strftime(format_str),
            self.frequency,
        )
        self.upload_data(data[currency_str]['prices'])
