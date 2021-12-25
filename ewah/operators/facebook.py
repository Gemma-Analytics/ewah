from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC
from ewah.utils.airflow_utils import datetime_utcnow_with_tz

from ewah.hooks.base import EWAHBaseHook as BaseHook

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

from datetime import datetime, timedelta

import inspect
import time


class EWAHFBOperator(EWAHBaseOperator):

    _NAMES = ["facebook", "fb"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: True,
    }

    class levels:
        ad = "ad"

    def __init__(
        self,
        insight_fields,
        level,
        data_since,
        account_ids=None,
        refresh_interval=timedelta(days=7),
        breakdowns=None,
        *args,
        **kwargs
    ):

        if isinstance(refresh_interval, int):
            refresh_interval = timedelta(days=refresh_interval)
        elif not isinstance(refresh_interval, timedelta):
            raise Exception("refresh_interval must be type timedelta or integer!")

        if kwargs.get("primary_key"):
            raise Exception("primary_key is set by operator!")

        if (
            not account_ids is None
            and not account_ids.__iter__
            and not isinstance(account_ids, (str, int))
        ):
            raise Exception(
                "account_ids must be a string, integer, or an iterable, such as a list,"
                + " of strings or integers!"
            )
        if account_ids is None or isinstance(account_ids, (str, int)):
            account_ids = [account_ids]

        if level == self.levels.ad:
            kwargs["primary_key"] = [
                "ad_id",
                "date_start",
                "date_stop",
            ] + (breakdowns or [])
            insight_fields += ["ad_id", "ad_name"]
            insight_fields = list(set(insight_fields))  # avoid duplications
        else:
            raise Exception("Specified level not supported!")

        allowed_insight_fields = [
            _attr[1]
            for _attr in [
                member
                for member in inspect.getmembers(
                    AdsInsights.Field,
                    lambda a: not (inspect.isroutine(a)),
                )
                if not (member[0].startswith("__") and member[0].endswith("__"))
            ]
        ]
        for i_f in insight_fields:
            if not i_f in allowed_insight_fields:
                raise Exception(
                    (
                        "Field {0} is not an accepted value for insight_fields! "
                        + "Accepted field values:\n\t{1}\n"
                    ).format(i_f, "\n\t".join(allowed_insight_fields))
                )

        assert isinstance(data_since, datetime), "data_from must be of type datetime!"

        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = "date_start"

        super().__init__(*args, **kwargs)

        self.data_since = data_since
        self.account_ids = account_ids
        self.insight_fields = insight_fields
        self.level = level
        self.breakdowns = breakdowns
        self.refresh_interval = refresh_interval

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_since = (
                self.get_max_value_of_column("date_start") - self.refresh_interval
            )
        else:
            data_since = self.data_since
        for account_id in self.account_ids or []:
            for batch in self.source_hook.get_data_in_batches(
                level=self.level,
                fields=self.insight_fields,
                data_from=data_since,
                data_until=datetime.now(),
                account_id=account_id,
                breakdowns=self.breakdowns,
            ):
                self.upload_data(batch)
