from datetime import datetime, timedelta
from yfinance import download

from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator
from ewah.utils.airflow_utils import datetime_utcnow_with_tz


class EWAHFXYFinanceOperator(EWAHBaseOperator):
    _NAMES = ["fx_yfinance"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
    }

    def __init__(
        self,
        currency_pair,  # iterable of length 2
        frequency="1d",  # 1d, 1wk, 1mo
        *args,
        **kwargs
    ):
        if not frequency in ("1d", "1wk", "1mo"):
            if frequency == "daily":
                frequency = "1d"
            elif frequency == "weekly":
                frequency = "1wk"
            elif frequency == "monthly":
                frequency = "1mo"
            else:
                raise Exception("Frequency must be one of: 1d, 1wk, 1mo")

        if not len(currency_pair) == 2:
            raise Exception(
                "currency_pair must be iterable of length 2 "
                + "containing the currency pair."
            )

        # No connection id required for this operator
        if kwargs.get("source_conn_id"):
            self.log.info(
                "source_conn_id is not required for operator! Ignoring argument."
            )
        kwargs["source_conn_id"] = None
        # If incremental, the primary key column is 'date'
        if kwargs.get("primary_key"):
            self.log.info("primary_key is fixed for this operator. Using the default.")
        kwargs["primary_key"] = ["date"]

        self.currency_pair = currency_pair
        self.frequency = frequency

        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        data_from = self.data_from or context["dag"].start_date
        # yfinance requires one day extra to also show the current day
        data_until = (self.data_until or datetime_utcnow_with_tz()) + timedelta(days=1)

        format_str = "%Y-%m-%d"
        currency_str = "{0}{1}=X".format(*self.currency_pair)
        data = download(
            currency_str,
            data_from.strftime(format_str),
            data_until.strftime(format_str),
            self.frequency,
            auto_adjust=False
        )
        data_list = []
        for index, row in data.iterrows():
            data_dict = {
                # UNIX timestamp is calculated, adding 82800 to match unix timestamp from yahoofinancials (legacy)
                'date': int(datetime.strptime(index.strftime('%Y-%m-%d'), '%Y-%m-%d').timestamp() + 82800),
                'high': float(row['High'].iloc[0]),
                'low': float(row['Low'].iloc[0]),
                'open': float(row['Open'].iloc[0]),
                'close': float(row['Close'].iloc[0]),
                'volume': float(row['Volume'].iloc[0]),
                'adjclose': float(row['Adj Close'].iloc[0]),
                'formatted_date': index.strftime('%Y-%m-%d')
            }
            data_list.append(data_dict)

        self.upload_data(data_list)
