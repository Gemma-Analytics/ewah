from ewah.hooks.base import EWAHBaseHook

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

from datetime import datetime, timedelta

import time


class EWAHFacebookHook(EWAHBaseHook):
    _ATTR_RELABEL = {
        "app_id": "login",
        "app_secret": "password",
        "api_version": "host",
        "account_id": "schema",
    }

    conn_name_attr = "ewah_facebook_conn_id"
    default_conn_name = "ewah_facebook_default"
    conn_type = "ewah_facebook"
    hook_name = "EWAH Facebook Connection"

    _DEFAULT_VERSION = "v13.0"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "extra"],
            "relabeling": {
                "host": "[Optional] API Version",
                "schema": "[Optional] Account ID",
                "login": "App ID",
                "password": "App Secret",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from wtforms import StringField
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget

        return {
            "extra__ewah_facebook__access_token": StringField(
                "Access Token", widget=BS3PasswordFieldWidget()
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def fb_init(self, account_id=None):
        account_id = account_id or self.conn.account_id or None
        if account_id:
            account_id = str(account_id)
            if not account_id.startswith("act_"):
                account_id = "act_{0}".format(account_id)
        api_version = self.conn.api_version or self._DEFAULT_VERSION
        if not api_version.startswith("v"):
            api_version = "v{0}".format(api_version)
        FacebookAdsApi.init(
            app_id=self.conn.app_id or None,
            app_secret=self.conn.app_secret or None,
            access_token=self.conn.access_token,
            api_version=api_version,
            account_id=account_id,
        )

    def get_data_in_batches(
        self,
        level,
        fields,
        data_from,
        data_until,
        breakdowns=None,
        account_id=None,
        batch_size=10000,
    ):
        if hasattr(data_from, "date"):
            data_from = data_from.date()
        if hasattr(data_until, "date"):
            data_until = data_until.date()
        self.fb_init(account_id=account_id)
        account_id = str(account_id or self.conn.account_id)
        if not account_id.startswith("act_"):
            account_id = "act_{0}".format(account_id)
        account_object = AdAccount(account_id)

        params = {
            "time_increment": 1,
            "level": level,
            "limit": 1000,
        }
        if breakdowns:
            if isinstance(breakdowns, list):
                breakdowns = ",".join(breakdowns)
            params["breakdowns"] = breakdowns

        data = []
        # maximum request of 90 days at once!
        while data_from <= data_until:
            request_until = min(data_until, data_from + timedelta(days=90))
            params["time_range"] = {
                "since": data_from.strftime("%Y-%m-%d"),
                "until": request_until.strftime("%Y-%m-%d"),
            }
            data_from = request_until + timedelta(days=1)
            self.log.info(
                "Requesting data for account_id={0} between {1} and {2}.".format(
                    str(account_id),
                    params["time_range"]["since"],
                    params["time_range"]["until"],
                )
            )
            async_job = account_object.get_insights_async(
                fields=fields,
                params=params,
            )

            job_read = async_job.api_get()
            while not job_read.get("async_status") in [
                "Job Completed",
                "Job Failed",
                "Job Skipped",
            ]:
                self.log.info(
                    "Asnyc job completion: {0}% (status: {1})".format(
                        str(job_read.get("async_percent_completion")),
                        str(job_read.get("async_status")),
                    )
                )
                time.sleep(5)
                job_read = async_job.api_get()

            time.sleep(1)
            assert (
                job_read.get("async_status") == "Job Completed"
            ), job_read.get_result()

            data += [
                {
                    k: datetime.strptime(v, "%Y-%m-%d").date()
                    if k in ("date_start", "date_stop")
                    else v
                    for k, v in datum.items()
                }
                for datum in list(job_read.get_result(params={"limit": 1000}))
            ]

            if len(data) >= batch_size:
                yield data
                data = []

        if data:
            yield data
