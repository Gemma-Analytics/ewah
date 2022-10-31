from ewah.hooks.base import EWAHBaseHook
from ewah.constants import EWAHConstants as EC

# from datetime import datetime, date, timedelta
import pendulum
import requests
import gzip
import json
import time


class EWAHAmazonAdsHook(EWAHBaseHook):
    """
    Implements the Amazon Ads API.
    """

    _ENDPOINTS_ADS_API = {  # TODO: validate in operator
        "NA": "https://advertising-api.amazon.com",
        "EU": "https://advertising-api-eu.amazon.com",
        "FE": "https://advertising-api-fe.amazon.com",
    }

    _ENDPOINTS_TOKENS = {
        "NA": "https://api.amazon.com/auth/o2/token",
        "EU": "https://api.amazon.co.uk/auth/o2/token",
        "FE": "https://api.amazon.co.jp/auth/o2/token",
    }

    _ADS_TYPES = ["sp", "sb", "sd"]  # TODO: validate in operator

    _ATTR_RELABEL = {}

    conn_name_attr = "ewah_amazon_ads_conn_id"
    default_conn_name = "ewah_amazon_ads_default"
    conn_type = "ewah_amazon_ads"
    hook_name = "EWAH Amazon Ads API Connection"

    @staticmethod
    def get_ui_field_behaviour():
        # Hide all and use custom fields only
        return {
            "hidden_fields": ["port", "schema", "extra", "host", "login", "password"],
            "relabeling": {},
        }

    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import (
            BS3TextFieldWidget,
            BS3PasswordFieldWidget,
        )
        from wtforms import StringField

        return {
            "extra__ewah_amazon_ads__lwa_client_id": StringField(
                "AWS LWA Client ID", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_amazon_ads__lwa_client_secret": StringField(
                "AWS LWA CLient Secret", widget=BS3PasswordFieldWidget()
            ),
            "extra__ewah_amazon_ads__region": StringField(
                "Amazon Ads Region (one of: NA, EU, FE)", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_amazon_ads__refresh_token": StringField(
                "Refresh Token", widget=BS3PasswordFieldWidget()
            ),
        }

    @property
    def access_token(self):
        if (
            not hasattr(self, "_access_token")
            or pendulum.now() > self._access_token_expires_at
        ):
            self.log.info("Requesting new access token...")
            requested_at = pendulum.now()
            url = self._ENDPOINTS_TOKENS[self.conn.region]
            payload = {
                "grant_type": "refresh_token",
                "client_id": self.conn.lwa_client_id,
                "client_secret": self.conn.lwa_client_secret,
                "refresh_token": self.conn.refresh_token,
            }
            response = requests.post(url, data=payload)
            assert response.status_code == 200, response.text
            self._access_token = response.json()["access_token"]
            self._access_token_expires_at = requested_at.add(
                seconds=response.json()["expires_in"] * 0.85
            )
            self.log.info(
                "New Amazon oauth access token is valid until {0}...".format(
                    self._access_token_expires_at.isoformat()
                )
            )
        return self._access_token

    def get_profile_ids(self):
        url = "/".join([self._ENDPOINTS_ADS_API[self.conn.region], "v2", "profiles"])
        headers = {
            "Amazon-Advertising-API-ClientId": self.conn.lwa_client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        assert response.status_code == 200, response.text
        return response.json()

    def get_report(
        self,
        date,
        ads_type,
        report_type,
        profile_id,
        additional_params=None,
    ):
        # profile_id - the account id to be used
        # ads_type - sp, sb, sd for sponsored products, brands, or display
        # report_type - e.g. keywords, targets

        self.log.info("Requesting report creation...")
        url = "/".join(
            [
                self._ENDPOINTS_ADS_API[self.conn.region],
                "v2",
                ads_type,
                report_type,
                "report",
            ]
        )
        headers = {
            "Amazon-Advertising-API-ClientId": self.conn.lwa_client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-Scope": str(profile_id),
            "Content-Type": "application/json",
        }
        params = {
            "reportDate": date.isoformat().replace("-", ""),
            "metrics": "cost,clicks,keywordId,keywordText",  # TODO: make kwarg
            **(additional_params or {})
            # "segment": "query",  # TODO: make kwarg
        }
        self.log.info(f"Creating report at {url}")
        response = requests.post(url, data=json.dumps(params), headers=headers)
        assert response.status_code == 202, response.text
        report_id = response.json()["reportId"]

        # Loop for report status
        wait_for = 1
        while True:
            url = "/".join(
                [self._ENDPOINTS_ADS_API[self.conn.region], "v2", "reports", report_id]
            )
            self.log.info(f"Pinging report status at {url}")
            response = requests.get(url, headers=headers)
            assert response.status_code == 200, response.text
            report_status = response.json()["status"]
            if report_status == "SUCCESS":
                break
            if report_status == "FAILURE":
                raise Exception(
                    f"Report failure!\n\nFull status request response:"
                    f"\n\n{response.text}\n\n"
                )
            if not report_status == "IN_PROGRESS":
                raise Exception(f"Invalid report status: {report_status}")
            self.log.info(f"In progress. Trying again in {wait_for}s...")
            time.sleep(wait_for)
            wait_for *= 2

        # Download data
        url = "/".join(
            [
                self._ENDPOINTS_ADS_API[self.conn.region],
                "v2",
                "reports",
                report_id,
                "download",
            ]
        )
        self.log.info(f"Downloading report from {url}")
        response = requests.get(url, headers=headers)
        assert response.status_code == 200, response.text
        report_data = json.loads(gzip.decompress(response.content).decode())

        return report_data
