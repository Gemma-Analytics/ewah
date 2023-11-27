from datetime import datetime
from yahoofinancials import YahooFinancials

from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator
from ewah.utils.airflow_utils import datetime_utcnow_with_tz


class EWAHFXOperator(EWAHBaseOperator):
    _NAMES = ["fx"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
    }

    def __init__(
        self,
        currency_pair,  # iterable of length 2
        frequency="daily",  # daily, weekly, or monthly
        *args,
        **kwargs
    ):
        if not frequency in ("daily", "weekly", "monthly"):
            raise Exception("Frequency must be one of: daily, weekly, monthly")

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
        data_until = self.data_until or datetime_utcnow_with_tz()

        format_str = "%Y-%m-%d"
        currency_str = "{0}{1}=X".format(*self.currency_pair)
        data = YahooFinancials([currency_str]).get_historical_price_data(
            data_from.strftime(format_str),
            data_until.strftime(format_str),
            self.frequency,
        )
        self.upload_data(data[currency_str]["prices"])
