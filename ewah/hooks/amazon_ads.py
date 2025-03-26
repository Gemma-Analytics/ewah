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

    def _make_request_with_backoff(self, request_func, max_retries=5):
        """
        Make a request with exponential backoff for rate-limited endpoints. Intially we take the retry-after value, but if we get multiple 429s in a row, we add an additional exponential backoff.
        """
        attempt = 0
        consecutive_429s = 0  # Track consecutive 429 responses
        base_wait = 1  # Base wait time for exponential backoff

        while attempt <= max_retries:
            response = request_func()
            
            # If successful response, return it
            if response.status_code == 200:
                consecutive_429s = 0
                return response
                
            # If rate limited (429), retry with backoff
            if response.status_code == 429:
                if attempt == max_retries:
                    break
                
                consecutive_429s += 1
                retry_after = int(response.headers.get('Retry-After', 1))
                
                if consecutive_429s > 2:
                    additional_backoff = base_wait * (2 ** (consecutive_429s - 2))
                    wait_time = retry_after + additional_backoff
                    self.log.info(
                        f"Multiple 429s detected. Adding backoff of {additional_backoff}s "
                        f"to Retry-After value of {retry_after}s"
                    )
                else:
                    wait_time = retry_after

                self.log.info(
                    f"Request throttled. Retrying in {wait_time} seconds "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
                attempt += 1
                continue
            
            # For any other error code, fail immediately
            assert False, (
                f"Request failed with status code {response.status_code}. "
                f"Response: {response.text}"
            )

        # If we've exhausted all retries on 429s, raise the last error
        assert False, (
            f"Request failed after {max_retries} retries due to rate limiting. "
            f"Status code: {response.status_code}, "
            f"Response: {response.text}"
        )

    def get_profile_ids(self):
        url = "/".join([self._ENDPOINTS_ADS_API[self.conn.region], "v2", "profiles"])
        headers = {
            "Amazon-Advertising-API-ClientId": self.conn.lwa_client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        response = self._make_request_with_backoff(
            lambda: requests.get(url, headers=headers)
        )
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

    def get_report_v3(
        self,
        start_date,
        end_date,
        ad_product,
        report_type,
        profile_id,
        additional_params=None,
    ):
        # profile_id - the account id to be used
        # ad_product - e.g. SPONSORED_PRODUCTS (sp), for now only sp
        # report_type - e.g. spCampaign, spAdvertisedProduct
        # additional_params - contains configuration params for API v3 like groupBy, columns, filters, etc.

        self.log.info("Requesting report creation...")
        url = "/".join(
            [
                self._ENDPOINTS_ADS_API[self.conn.region],
                "reporting",
                "reports",
            ]
        )
        headers = {
            "Amazon-Advertising-API-ClientId": self.conn.lwa_client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-Scope": str(profile_id),
            "Content-Type": "application/json",
        }
        params = {
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
            "configuration": {
                "adProduct": ad_product,
                "reportTypeId": report_type,
                "timeUnit": "DAILY",
                "format": "GZIP_JSON",
                **(additional_params),
            },
        }
        self.log.info(f"Creating report at {url}")
        response = self._make_request_with_backoff(
            lambda: requests.post(url, data=json.dumps(params), headers=headers)
        )
        report_id = response.json()["reportId"]

        # Loop for report status
        wait_for = 15 # min wait time from experience
        while True:
            url = "/".join(
                [
                    self._ENDPOINTS_ADS_API[self.conn.region],
                    "reporting",
                    "reports",
                    report_id,
                ]
            )
            self.log.info(f"Pinging report status at {url}")
            # Update headers with fresh token on each request
            headers["Authorization"]=f"Bearer {self.access_token}"
            response = self._make_request_with_backoff(
                lambda: requests.get(url, headers=headers)
            )
            report_status = response.json()["status"]
            if report_status == "COMPLETED":
                break
            if report_status == "FAILURE":
                raise Exception(
                    f"Report failure!\n\nFull status request response:"
                    f"\n\n{response.text}\n\n"
                )
            if not report_status in ("PROCESSING", "PENDING"):
                raise Exception(f"Invalid report status: {report_status}")
            self.log.info(f"In progress. Trying again in {wait_for}s...")
            time.sleep(wait_for)
            wait_for *= 2

        # Download data
        download_url = response.json()["url"]
        self.log.info(f"Downloading report from {download_url}")
        download = self._make_request_with_backoff(
            lambda: requests.get(download_url)
        )
        report_data = json.loads(gzip.decompress(download.content).decode())

        return report_data

    def get_report_dsp(
        self,
        account_id,
        report_date_start,
        report_date_end,
        report_type,
        metrics=None,
        dimensions=None,
    ):
        self.log.info("Requesting report creation...")
        url = "/".join(
            [
                self._ENDPOINTS_ADS_API[self.conn.region],
                "accounts",
                account_id,
                "dsp",
                "reports",
            ]
        )
        headers = {
            "User-Agent": "EWAH",
            "Accept": "application/vnd.dspcreatereports.v3+json",
            "Amazon-Advertising-API-ClientId": self.conn.lwa_client_id,
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        body = {
            "startDate": report_date_start,
            "endDate": report_date_end,
            "format": "JSON",
            "type": report_type,
            "timeUnit": "DAILY",
        }
        if metrics:
            body["metrics"] = metrics
        if dimensions:
            body["dimensions"] = dimensions

        response = requests.post(url, headers=headers, json=body)
        assert response.status_code == 202, response.text
        report_id = response.json()["reportId"]

        # Wait until report is ready
        url = "/".join([url, report_id])
        headers["Accept"] = "application/vnd.dspgetreports.v3+json"
        # Loop for report status
        wait_for = 1
        while True:
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

        url = response.json()["location"]
        self.log.info("Downloading report...")
        response = requests.get(url)
        assert response.status_code == 200, response.text
        return json.loads(response.content.decode())
