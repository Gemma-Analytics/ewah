from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC
from ewah.ewah_utils.airflow_utils import datetime_utcnow_with_tz

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
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: True,
    }

    class levels:
        ad = "ad"

    def __init__(
        self,
        account_ids,
        insight_fields,
        level,
        time_increment=1,
        breakdowns=None,
        execution_waittime_seconds=15,  # wait for a while before execution
        #   between account_ids to avoid hitting rate limits during backfill
        pagination_limit=1000,
        async_job_read_frequency_seconds=5,
        *args,
        **kwargs
    ):

        if kwargs.get("update_on_columns"):
            raise Exception("update_on_columns is set by operator!")

        if not account_ids.__iter__:
            raise Exception(
                "account_ids must be an iterable, such as a list,"
                + " of strings or integers!"
            )

        if level == self.levels.ad:
            kwargs["update_on_columns"] = [
                "ad_id",
                "date_start",
                "date_stop",
            ] + (breakdowns or [])
            insight_fields += ["ad_id", "ad_name"]
            insight_fields = list(set(insight_fields))
        else:
            raise Exception("Specified level not supported!")

        if not (
            (type(time_increment) == str and time_increment in ["monthly", "all_days"])
            or (
                type(time_increment) == int
                and time_increment >= 1
                and time_increment <= 1
            )
        ):
            raise Exception(
                "time_increment must either be an integer "
                + 'between 1 and 90, or a string of either "monthly" '
                + 'or "all_days". Recommended and default is the integer 1.'
            )

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

        super().__init__(*args, **kwargs)

        credentials = BaseHook.get_connection(self.source_conn_id)
        extra = credentials.extra_dejson

        # Note: app_secret is not always required!
        if not extra.get("app_id"):
            raise Exception('Connection extra must contain an "app_id"!')
        if not extra.get("access_token", credentials.password):
            raise Exception(
                'Connection extra must contain an "access_token" '
                + "if it is not saved as the connection password!"
            )

        self.credentials = {
            "app_id": extra.get("app_id"),
            "app_secret": extra.get("app_secret"),
            "access_token": extra.get("access_token", credentials.password),
        }

        self.account_ids = account_ids
        self.insight_fields = insight_fields
        self.level = level
        self.time_increment = time_increment
        self.breakdowns = breakdowns
        self.execution_waittime_seconds = execution_waittime_seconds
        self.pagination_limit = pagination_limit
        self.async_job_read_frequency_seconds = async_job_read_frequency_seconds

    def _clean_response_data(self, response):
        return [dict(datum) for datum in list(response)]

    def ewah_execute(self, context):
        time_range = {
            "since": self.data_from.strftime("%Y-%m-%d"),
            "until": self.data_until.strftime("%Y-%m-%d"),
        }

        FacebookAdsApi.init(**self.credentials)
        params = {
            "time_range": time_range,
            "time_increment": self.time_increment,
            "level": self.level,
            "limit": self.pagination_limit,
        }
        if self.breakdowns:
            params.update({"breakdowns": ",".join(self.breakdowns)})

        for account_id in self.account_ids:
            if self.execution_waittime_seconds:
                self.log.info(
                    "Delaying execution by {0} seconds...".format(
                        str(self.execution_waittime_seconds),
                    )
                )
                now = datetime_utcnow_with_tz()
                while datetime_utcnow_with_tz() < (
                    now + timedelta(seconds=self.execution_waittime_seconds)
                ):
                    time.sleep(1)

            account_object = AdAccount("act_{0}".format(str(account_id)))
            self.log.info(
                ("Requesting data for account_id={0} between {1} and {2}.").format(
                    str(account_id),
                    time_range["since"],
                    time_range["until"],
                )
            )

            async_job = account_object.get_insights_async(
                fields=self.insight_fields,
                params=params,
            )
            job_remote_read = async_job.api_get()
            done_status = [
                "Job Completed",
                "Job Failed",
                "Job Skipped",
            ]
            while not (job_remote_read.get("async_status") in done_status):
                self.log.info(
                    "Asnyc job completion: {0}% (status: {1})".format(
                        str(job_remote_read.get("async_percent_completion")),
                        str(job_remote_read.get("async_status")),
                    )
                )
                time.sleep(self.async_job_read_frequency_seconds)
                job_remote_read = async_job.api_get()

            time.sleep(1)
            assert job_remote_read.get("async_status") == "Job Completed"
            data = self._clean_response_data(
                async_job.get_result(
                    params={"limit": self.pagination_limit},
                )
            )
            self.upload_data(data)
