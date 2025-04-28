"""
Airbyte's implementation of this connector has been an important source of information
in the process of building this EWAH connector. Check out Airbyte for more info!
"""

from ewah.hooks.base import EWAHBaseHook
from ewah.constants import EWAHConstants as EC

from datetime import datetime, date, timedelta
from dateutil.parser import parse as parse_datetime
from io import StringIO
import xml.etree.ElementTree as ET
import urllib.parse
import pendulum
import hashlib
import hmac
import boto3
import requests
import time
import pytz
import copy
import gzip
import json
import csv


class EWAHAmazonSellerCentralHook(EWAHBaseHook):
    """
    Implements the Amazon Seller Portal API (SP-API).

    Currently, only the Reporting API is implemented.

    Since the report types vary vastly in implementation, each report type
    has its own method to properly load it. They are supported by common
    methods for authentication and getting the raw report contents.
    """

    # Allowed reports with the alias and various settings
    _REPORT_METADATA = {
        "orders": {
            "report_type": "GET_XML_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL",
            "report_options": {},
            "method_name": "get_order_data_from_reporting_api",
            "primary_key": ["AmazonOrderID"],
            "subsequent_field": "LastUpdatedDate",
            "accepted_strategies": [EC.ES_INCREMENTAL, EC.ES_SUBSEQUENT],
        },
        "sales_and_traffic": {
            "report_type": "GET_SALES_AND_TRAFFIC_REPORT",
            "report_options": {
                "dateGranularity": ["DAY", "WEEK", "MONTH"],
                "asinGranularity": ["PARENT", "CHILD", "SKU"],
            },
            "method_name": "get_sales_and_traffic_data",
            "primary_key": [
                "parentAsin",
                "childAsin",
                "date",
            ],
            "subsequent_field": "date",
            "accepted_strategies": [EC.ES_INCREMENTAL, EC.ES_SUBSEQUENT],
        },
        "fba_returns": {
            # This report does not have a primary key, hence cannot use incremental
            # loading for it - must use full refresh every time
            "report_type": "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA",
            "report_options": {},
            "method_name": "get_fba_returns_data",
            "primary_key": None,
            "subsequent_field": None,
            "accepted_strategies": [EC.ES_FULL_REFRESH],
        },
        "listings": {  # Full-refresh only
            "report_type": "GET_MERCHANT_LISTINGS_ALL_DATA",
            "report_options": {},
            "method_name": "get_listings",
            "primary_key": None,
            "subsequent_field": None,
            "accepted_strategies": [EC.ES_FULL_REFRESH],
            "ewah_options": {
                "add_bsr": [True, False],
            },
        },
        "inventory": {  # Full-refresh only
            "report_type": "GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA",
            "report_options": {},
            "method_name": "get_inventory",
            "primary_key": None,
            "subsequent_field": None,
            "accepted_strategies": [EC.ES_FULL_REFRESH],
        },
        "fee_preview": {  # Full-refresh only
            "report_type": "GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA",
            "report_options": {},
            "method_name": "get_fee_preview",
            "primary_key": None,
            "subsequent_field": None,
            "accepted_strategies": [EC.ES_FULL_REFRESH],
        },
    }

    _ATTR_RELABEL = {}

    conn_name_attr = "ewah_amazon_seller_central_conn_id"
    default_conn_name = "ewah_amazon_seller_central_default"
    conn_type = "ewah_amazon_seller_central"
    hook_name = "EWAH Amazon Seller Central Connection"

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
            "extra__ewah_amazon_seller_central__aws_access_key_id": StringField(
                "AWS Access Key ID (Optional)", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_amazon_seller_central__aws_secret_access_key": StringField(
                "AWS Secret Access Key (Optional)", widget=BS3PasswordFieldWidget()
            ),
            "extra__ewah_amazon_seller_central__aws_arn_role": StringField(
                "AWS ARN - Role (Optional)", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_amazon_seller_central__lwa_client_id": StringField(
                "LoginWithAmazon (LWA) Client ID", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_amazon_seller_central__lwa_client_secret": StringField(
                "LoginWithAmazon (LWA) Client Secret", widget=BS3PasswordFieldWidget()
            ),
            "extra__ewah_amazon_seller_central__refresh_token": StringField(
                "Refresh Token", widget=BS3PasswordFieldWidget()
            ),
        }

    @staticmethod
    def get_marketplace_details_tuple(marketplace_region):
        base_url = "https://sellingpartnerapi"
        return {
            "AE": (f"{base_url}-eu.amazon.com", "A2VIGQ35RCS4UG", "eu-west-1"),
            "DE": (f"{base_url}-eu.amazon.com", "A1PA6795UKMFR9", "eu-west-1"),
            "PL": (f"{base_url}-eu.amazon.com", "A1C3SOZRARQ6R3", "eu-west-1"),
            "EG": (f"{base_url}-eu.amazon.com", "ARBP9OOSHTCHU", "eu-west-1"),
            "ES": (f"{base_url}-eu.amazon.com", "A1RKKUPIHCS9HS", "eu-west-1"),
            "FR": (f"{base_url}-eu.amazon.com", "A13V1IB3VIYZZH", "eu-west-1"),
            "IN": (f"{base_url}-eu.amazon.com", "A21TJRUUN4KGV", "eu-west-1"),
            "IT": (f"{base_url}-eu.amazon.com", "APJ6JRA9NG5V4", "eu-west-1"),
            "NL": (f"{base_url}-eu.amazon.com", "A1805IZSGTT6HS", "eu-west-1"),
            "SA": (f"{base_url}-eu.amazon.com", "A17E79C6D8DWNP", "eu-west-1"),
            "SE": (f"{base_url}-eu.amazon.com", "A2NODRKZP88ZB9", "eu-west-1"),
            "TR": (f"{base_url}-eu.amazon.com", "A33AVAJ2PDY3EV", "eu-west-1"),
            "UK": (f"{base_url}-eu.amazon.com", "A1F83G8C2ARO7P", "eu-west-1"),
            "AU": (f"{base_url}-fe.amazon.com", "A39IBJ37TRP1C6", "us-west-2"),
            "JP": (f"{base_url}-fe.amazon.com", "A1VC38T7YXB528", "us-west-2"),
            "SG": (f"{base_url}-fe.amazon.com", "A19VAU5U5O7RUS", "us-west-2"),
            "US": (f"{base_url}-na.amazon.com", "ATVPDKIKX0DER", "us-east-1"),
            "BR": (f"{base_url}-na.amazon.com", "A2Q3Y263D00KWC", "us-east-1"),
            "CA": (f"{base_url}-na.amazon.com", "A2EUQ1WTGCTBG2", "us-east-1"),
            "MX": (f"{base_url}-na.amazon.com", "A1AM78C64UM0Y8", "us-east-1"),
            "GB": (f"{base_url}-eu.amazon.com", "A1F83G8C2ARO7P", "eu-west-1"),
        }[marketplace_region]

    @classmethod
    def get_cleaner_callables(cls):
        def string_to_date(row):
            for key, value in row.items():
                if key.lower().endswith("date") and not isinstance(
                    value, (date, datetime)
                ):
                    # Cast all dates
                    if value:  # Don't parse NoneType or empty string
                        row[key] = parse_datetime(value)
            if row.get("parentAsin") and not row.get("childAsin"):
                row["childAsin"] = "n.a."
            return row

        return string_to_date

    @classmethod
    def validate_marketplace_region(
        cls, marketplace_region, raise_on_failure=False, allow_lists=False
    ):
        # Validate whether a region exists. Return boolean or raise exception.

        # Optionally check all regions in a list of regions
        if allow_lists and isinstance(marketplace_region, list):
            for region in marketplace_region:
                if not cls.validate_marketplace_region(
                    region, raise_on_failure=raise_on_failure
                ):
                    return False
            # If execution gets here, it validated all regions of the list successfully
            return True

        # Wrong data type, not valid by definition
        elif not isinstance(marketplace_region, str):
            return False

        # Now validate!
        try:
            cls.get_marketplace_details_tuple(marketplace_region)
        except:
            if raise_on_failure:
                raise
            else:
                return False
        finally:
            return True

    @staticmethod
    def _sign_msg(key, msg):
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    @property
    def access_token(self):
        if (
            not hasattr(self, "_access_token")
            or pendulum.now() > self._access_token_expires_at
        ):
            self.log.info("Requesting new access token...")
            requested_at = pendulum.now()
            url = "https://api.amazon.com/auth/o2/token"
            payload = {
                "grant_type": "refresh_token",
                "client_id": self.conn.lwa_client_id,
                "client_secret": self.conn.lwa_client_secret,
                "refresh_token": self.conn.refresh_token,
            }
            response = requests.post(url, data=payload)
            assert response.status_code == 200, response.text
            response_data = response.json()
            self._access_token = response_data["access_token"]
            self._access_token_expires_at = requested_at.add(
                # add a 15% error margin to avoid expiration during a call
                seconds=(response_data["expires_in"] * 0.85),
            )
            self.log.info(
                "New Amazon oauth access token is valid until {0}...".format(
                    self._access_token_expires_at.isoformat()
                )
            )
        return self._access_token

    @property
    def use_legacy_authentication(self):
        legacy_secrets = [
            self.conn.aws_access_key_id,
            self.conn.aws_secret_access_key,
            self.conn.aws_arn_role,
        ]
        if any([secret in (None, "") for secret in legacy_secrets]):
            if all([secret in (None, "") for secret in legacy_secrets]):
                return False
            raise Exception(
                "Some, but not all, legacy secrets are used! Either use none at all or fill all out (AWS access key, secret key, arn role)"
            )
        return True

    @property
    def boto3_role_credentials(self):
        if (
            not hasattr(self, "_boto3_role_credentials")
            or pendulum.now() > self._boto3_session_expires_at
        ):
            self.log.info("Logging in to AWS...")
            boto3_client = boto3.client(
                "sts",
                aws_access_key_id=self.conn.aws_access_key_id,
                aws_secret_access_key=self.conn.aws_secret_access_key,
            )
            role = boto3_client.assume_role(
                RoleArn=self.conn.aws_arn_role, RoleSessionName="guid"
            )
            self._boto3_role_credentials = role["Credentials"]
            # Add a 15% error margin to avoid expiration during a call
            self._boto3_session_expires_at = pendulum.now() + 0.85 * (
                role["Credentials"]["Expiration"] - pendulum.now()
            )
            self.log.info(
                "Logged in to AWS! Login expires at: {0}".format(
                    self._boto3_session_expires_at.isoformat()
                )
            )
        return self._boto3_role_credentials

    @property
    def aws_session_token(self):
        return self.boto3_role_credentials.get("SessionToken")

    @property
    def aws_session_secret_access_key(self):
        return self.boto3_role_credentials.get("SecretAccessKey")

    @property
    def aws_session_access_key_id(self):
        return self.boto3_role_credentials.get("AccessKeyId")

    def generate_request_headers(self, url, method, region, params=None, body=None):
        # Returns the headers dict to authorize a request for the SP-API
        if not self.use_legacy_authentication:
            return {
                "x-amz-access-token": self.access_token,
                "Content-Type": "application/json",
            }

        # Below is the legacy authentication header generation

        assert method in ["GET", "POST"]
        params = params or {}

        current_ts = pendulum.now("utc")
        amz_date = current_ts.strftime("%Y%m%dT%H%M%SZ")
        datestamp = current_ts.strftime("%Y%m%d")
        url_parsed = urllib.parse.urlparse(url)
        uri = urllib.parse.quote(url_parsed.path)
        host = url_parsed.hostname
        service = "execute-api"

        canonical_querystring = "&".join(
            map(
                lambda param: "=".join(param),
                sorted(
                    [
                        [str(key), urllib.parse.quote_plus(str(value))]
                        for key, value in params.items()
                    ]
                ),
            )
        )

        headers_to_sign = {
            "host": host,
            "x-amz-date": amz_date,
            "x-amz-security-token": self.aws_session_token,
        }
        ordered_headers = dict(sorted(headers_to_sign.items(), key=lambda h: h[0]))
        canonical_headers = "".join(
            map(lambda h: ":".join(h) + "\n", ordered_headers.items())
        )
        signed_headers = ";".join(ordered_headers.keys())

        if body:
            payload_hash = hashlib.sha256(
                json.dumps(dict(sorted(body.items(), key=lambda h: h[0]))).encode(
                    "utf-8"
                )
            ).hexdigest()
        else:
            payload_hash = hashlib.sha256("".encode("utf-8")).hexdigest()

        canonical_request = "\n".join(
            [
                method,
                uri,
                canonical_querystring,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )

        credential_scope = "/".join([datestamp, region, service, "aws4_request"])
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )

        datestamp_signed = self._sign_msg(
            ("AWS4" + self.aws_session_secret_access_key).encode("utf-8"), datestamp
        )
        region_signed = self._sign_msg(datestamp_signed, region)
        service_signed = self._sign_msg(region_signed, service)
        aws4_request_signed = self._sign_msg(service_signed, "aws4_request")
        signature = hmac.new(
            aws4_request_signed, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        authorization_header = (
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}".format(
                self.aws_session_access_key_id,
                credential_scope,
                signed_headers,
                signature,
            )
        )

        return {
            "content-type": "application/json",
            "host": host,
            "user-agent": "python-requests",
            "x-amz-access-token": self.access_token,
            "x-amz-date": amz_date,
            "x-amz-security-token": self.aws_session_token,
            "authorization": authorization_header,
        }

    def get_report_data(
        self,
        marketplace_region,
        report_name,
        data_from=None,
        data_until=None,
        report_options=None,
    ):
        # This method calls the reporting API to fetch data on a report
        # Use this method when writing the individual reports' fetching methods
        # Returns the report content unaltered (only decompressed, if applicable)

        report_metadata = self._REPORT_METADATA[report_name]
        report_type = report_metadata["report_type"]

        endpoint, marketplace_id, region = self.get_marketplace_details_tuple(
            marketplace_region
        )
        url = endpoint + "/reports/2021-06-30/reports"
        body = {
            "marketplaceIds": [marketplace_id],
            "reportType": report_type,
        }

        if report_options:
            available_options = report_metadata.get("report_options", {})
            assert isinstance(report_options, dict), "report_options must be a dic!"
            for option, value in report_options.items():
                assert (
                    option in available_options.keys()
                ), f"Invalid report option '{option}'!"
                assert (
                    value in available_options[option]
                ), f"Invalid value '{value}' for option '{option}'!"
            body["reportOptions"] = report_options

        if data_from:
            if isinstance(data_from, (date, datetime)):
                data_from = data_from.isoformat()
            body["dataStartTime"] = data_from

        if data_until:
            if isinstance(data_until, (date, datetime)):
                data_until = data_until.isoformat()
            body["dataEndTime"] = data_until

        # The order of the items in the body is relevant for proper authorization
        body = dict(sorted(body.items(), key=lambda h: h[0]))
        self.log.info("Creating the report via POST request now... " f"Payload: {body}")
        response = requests.post(
            url,
            json=body,
            headers=self.generate_request_headers(
                url=url, method="POST", region=region, body=body
            ),
        )
        assert response.status_code == 202, response.text
        report_id = response.json()["reportId"]

        # Fetch the report Part 1: Get the document ID
        self.log.info(f"Report created, ID: {report_id}.")
        url = endpoint + "/reports/2021-06-30/reports/" + report_id
        tries = 0
        sleep_base = 5
        sleep_for = sleep_base
        done = False
        time.sleep(sleep_for)
        while tries < 10:
            tries += 1
            # Exponential increase of the time we are waiting
            sleep_for = sleep_base * (2**tries)
            self.log.info(f"Trying ({tries}/10) to fetch the document id...")
            response = requests.get(
                url,
                headers=self.generate_request_headers(
                    url=url, method="GET", region=region
                ),
            )
            assert response.status_code == 200, response.text
            status = response.json().get("processingStatus")
            if status == "DONE":
                done = True
                break
            elif status in ("IN_PROGRESS", "IN_QUEUE"):
                # Wait a bit, try again
                pass
            elif status == "CANCELLED":
                # There is no data to report on
                return
            elif status == "FATAL":
                raise Exception("FATAL ERROR! Request response: " + response.text)
            else:
                raise Exception(
                    "Unexpected processing status! Request response: " + response.text
                )
            self.log.info(
                f"Unable to fetch document, status: {status}."
                f" Waiting for {sleep_for}s..."
            )
            time.sleep(sleep_for)

        assert response.json().get("processingStatus") == "DONE", response.text
        report_document_id = response.json()["reportDocumentId"]

        # Fetch the report Part 2: Get the document url
        self.log.info(
            f"Got document ID: {report_document_id}." " Fetching document url now."
        )
        url = endpoint + "/reports/2021-06-30/documents/" + report_document_id
        response = requests.get(
            url,
            headers=self.generate_request_headers(url=url, method="GET", region=region),
        )
        assert response.status_code == 200, response.text
        response_data = response.json()
        assert (
            response_data.get("compressionAlgorithm") is None
            or response_data["compressionAlgorithm"] == "GZIP"
        ), "Invalid Compression Algorithm: {0}".format(
            response_data.get("compressionAlgorithm")
        )
        is_gzip = response_data.get("compressionAlgorithm") == "GZIP"
        document_url = response_data["url"]

        # Fetch the report Part 3: Download the document
        self.log.info(f"Got document URL. Now downloading document.")
        document_response = requests.get(document_url, allow_redirects=True)
        # Fix check (might be temporarily):
        # The compressionAlgorithm parameter is not reliable at the moment.
        # Its set, but the content is not always zipped, which we check here.
        is_zipped = document_response.content.startswith(b"\x1f\x8b")

        assert document_response.status_code == 200, document_response.text
        if is_zipped:
            document_content = gzip.decompress(document_response.content)
        else:
            document_content = document_response.content

        return document_content

    def get_order_data_from_reporting_api(
        self,
        marketplace_region,
        report_name,
        data_from,
        data_until,
        report_options=None,
        ewah_options=None,
        batch_size=10000,
    ):
        if (data_until - data_from) > timedelta(days=29):
            # Special case: Order reports can't be fetched
            # for a time period greater than 30 days.
            # Loop over the entire period in steps of 29 days.
            while data_from < data_until:
                for batch in self.get_order_data_from_reporting_api(
                    marketplace_region=marketplace_region,
                    report_name=report_name,
                    data_from=data_from,
                    data_until=min(data_until, data_from + timedelta(days=29)),
                    report_options=report_options,
                    batch_size=batch_size,
                ):
                    yield batch
                data_from += timedelta(days=29)
            # we're done here after looping over the period
            return

        self.log.info(
            "Fetching order data from {0} to {1}...".format(
                data_from.isoformat(), data_until.isoformat()
            )
        )

        def simple_xml_to_json(xml, depth=1):
            # Takes xml and turns it to a (nested) dictionary
            response = {}
            scalars = [
                "AmazonOrderID",
                "MerchantOrderID",
                "OrderStatus",
                "SalesChannel",
                "IsBusinessOrder",
                "IsIba",
                "LastUpdatedDate",
                "PurchaseDate",
            ]
            for child in list(xml):
                if not response.get(child.tag):
                    # Everything becomes a list in order to be consistent,
                    # even if length is always 1 for some children
                    response[child.tag] = []
                if len(list(child)) > 0:
                    # child is also an object
                    response[child.tag].append(simple_xml_to_json(child, depth + 1))
                else:
                    # tag is a scalar, add it
                    if (  # depth == 1 is Message, 2 is Order
                        depth == 3 and child.tag in scalars
                    ):
                        # Special cases - these are never lists
                        response[child.tag] = child.text
                    else:
                        response[child.tag].append(child.text)
            return response

        # Note: get_report_data may return None if there is no new data!
        data_string = (
            self.get_report_data(
                marketplace_region,
                report_name,
                data_from,
                data_until,
                report_options,
            )
            or b""
        ).decode()
        if data_string:
            self.log.info("Turning response XML into JSON...")
            raw_data = simple_xml_to_json(ET.fromstring(data_string)).get("Message", [])
        else:
            # No data to provide
            raw_data = []

        # Respect batch size kwarg and prettify data structure
        data = []
        i = 0
        while raw_data:
            i += 1
            # Due to the conversion logic, Message is a Dict of which we only want
            # Order, and Order is a list that is always of size 1.
            # We only want a list of Order as final result, though.
            data.append(raw_data.pop(0)["Order"][0])
            if i == batch_size:
                yield data
                data = []
                i = 0
        if data:
            yield data

    def get_sales_and_traffic_data(
        self,
        marketplace_region,
        report_name,
        data_from,
        data_until,
        report_options=None,
        ewah_options=None,
        batch_size=10000,  # is ignored in this specific function
    ):
        delta_day = timedelta(days=1)

        # This report fetches data in full day periods
        if isinstance(data_from, datetime):
            data_from = data_from.date()
        if isinstance(data_until, datetime):
            data_until = data_until.date()

        while True:
            # Sales and traffic report needs to be requested individually per day
            started = datetime.now()  # used in the next iteration, if applicable
            data_from_dt = data_from
            if isinstance(data_from_dt, pendulum.Date):
                # Uploader class doesn't like Pendulum (data_from_dt is added to data)
                data_from_dt = date(
                    data_from_dt.year,
                    data_from_dt.month,
                    data_from_dt.day,
                )

            # Note that if no data is returned (e.g., timeframe too early), the get_report_data method returns None
            data_raw = (
                self.get_report_data(
                    marketplace_region,
                    report_name,
                    data_from,
                    data_from,
                    report_options,
                )
                or b""
            ).decode()
            data = json.loads(data_raw or "{}").get("salesAndTrafficByAsin", [])
            for datum in data:
                # add the requested day to all rows
                datum["date"] = data_from_dt
            yield data

            data_from += delta_day
            if data_from > data_until:
                break

            self.log.info("Delaying execution for up to a minute...")
            # Delaying to avoid hitting API request rate limits
            # --> Wait 1 minute between iterations
            time.sleep(
                max(
                    0,
                    (started + timedelta(seconds=60) - datetime.now()).total_seconds(),
                )
            )

    def get_fba_returns_data(
        self,
        marketplace_region,
        report_name,
        data_from,
        data_until,
        report_options=None,
        ewah_options=None,
        batch_size=10000,
    ):
        self.log.info(
            "Fetching FBA returns data from {0} to {1}...".format(
                data_from.isoformat(), data_until.isoformat()
            )
        )

        # Note: This report is only available as full refresh, however
        # it still requires data_from (ideally, static) and data_until
        # (ideally, current timestamp).
        assert data_from, "Requires a minimum date parameter!"
        # data_until is defaulted to current timestamp

        # Return data from CSV in batches
        data_io = StringIO(
            self.get_report_data(
                marketplace_region,
                report_name,
                data_from,
                data_until or datetime.utcnow().replace(tzinfo=pytz.utc),
                report_options,
            ).decode()
        )  # TODO: check if latin-1?
        csv_reader = csv.DictReader(data_io, delimiter="\t")
        data = []
        i = 0
        for row in csv_reader:
            i += 1
            data.append(row)
            if i == batch_size:
                yield data
                i = 0
                data = []
        if data:
            yield data

    def get_listings(
        self,
        marketplace_region,
        report_name,
        data_from,
        data_until,
        report_options=None,
        ewah_options=None,
        batch_size=10000,
    ):
        self.log.info("Fetching Listings. Ignoring datetimes if any.")
        data_io = StringIO(
            self.get_report_data(
                marketplace_region,
                report_name,
                None,
                None,
                report_options,
            ).decode("latin-1")
        )
        csv_reader = csv.DictReader(data_io, delimiter="\t")
        data = []
        i = 0
        for row in csv_reader:
            i += 1
            if ewah_options and ewah_options.get("add_bsr"):
                # Make a request to add the BSR at this point
                asin = row.get("asin1")
                if asin:
                    self.log.info("Fetching additional catalogue data for a listing...")
                    row.update(self.get_listing_details(marketplace_region, asin) or {})
            data.append(row)
            if i == batch_size:
                yield data
                i = 0
                data = []
        if data:
            yield data

    def get_inventory(
        self,
        marketplace_region,
        report_name,
        data_from,
        data_until,
        report_options=None,
        ewah_options=None,
        batch_size=10000,
    ):
        self.log.info("Fetching Inventory. Ignoring datetimes if any.")
        data_io = StringIO(
            self.get_report_data(
                marketplace_region,
                report_name,
                None,
                None,
                report_options,
            ).decode("latin-1")
        )
        csv_reader = csv.DictReader(data_io, delimiter="\t")
        data = []
        i = 0
        for row in csv_reader:
            i += 1
            data.append(row)
            if i == batch_size:
                yield data
                i = 0
                data = []
        if data:
            yield data

    def get_fee_preview(
        self,
        marketplace_region,
        report_name,
        data_from,
        data_until,
        report_options=None,
        ewah_options=None,
        batch_size=10000,
    ):
        self.log.info("Fetching Fee Preview Report...")

        # The FBA Fee Preview Report (GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA)
        # "contains the estimated Amazon Selling and Fulfillment Fees for the seller's FBA
        # inventory with active offers. The content is updated at least once every
        # 72 hours. To successfully generate a report, specify the StartDate parameter
        # for a minimum 72 hours prior to NOW and EndDate to NOW."
        # https://developer-docs.amazon.com/sp-api/docs/report-type-values-fba#fba-payments-reports

        if not data_from:
            data_from = datetime.utcnow().replace(tzinfo=pytz.utc) - timedelta(hours=72)
            self.log.info(f"No data_from provided, setting to 72 hours ago: {data_from}")
        else:
            self.log.info(f"Using provided data_from: {data_from}")

        data_io = StringIO(
            self.get_report_data(
                marketplace_region,
                report_name,
                data_from,
                data_until,
                report_options,
            ).decode("latin-1")
        )
        csv_reader = csv.DictReader(data_io, delimiter="\t")
        data = []
        i = 0
        for row in csv_reader:
            i += 1
            data.append(row)
            if i == batch_size:
                yield data
                i = 0
                data = []
        if data:
            yield data

    def get_data_from_reporting_api_in_batches(
        self,
        marketplace_region,
        report_name,
        data_from=None,
        data_until=None,
        report_options=None,
        ewah_options=None,
        batch_size=10000,
    ):
        error_msg = """Invalid report name {1}! Valid options:
        \n\t- {0}
        """.format(
            "\n\t- ".join(self._REPORT_METADATA.keys()),
            report_name,
        )
        assert report_name in self._REPORT_METADATA.keys(), error_msg

        if isinstance(marketplace_region, list):
            # Special case - multiple marketplace_regions!
            # If we have a list of marketplace_regions, call self
            # once per region and add the region to the data.
            for region in marketplace_region:
                for batch in self.get_data_from_reporting_api_in_batches(
                    marketplace_region=region,
                    report_name=report_name,
                    data_from=data_from,
                    data_until=data_until,
                    report_options=report_options,
                    ewah_options=ewah_options,
                    batch_size=batch_size,
                ):
                    for datum in batch:
                        datum["ewah_marketplace_region"] = region
                    yield batch
            return
        elif not isinstance(marketplace_region, str):
            raise Exception("'marketplace_region' must be string or list of strings!")

        self.log.info(
            f"Fetching report '{report_name}' for region '{marketplace_region}' "
            f"between {data_from} and {data_until}.",
        )

        method = getattr(self, self._REPORT_METADATA[report_name]["method_name"])
        data = []
        for batch in method(
            marketplace_region,
            report_name,
            data_from,
            data_until,
            report_options,
            ewah_options,
        ):
            data += batch
            if len(data) >= batch_size:
                yield data
                data = []
        if data:
            # Last batch may otherwise not be yielded if below threshold of batch_size
            yield data

    def get_listing_details(self, marketplace_region, asin):
        # For a single ASIN, retrieve catalogue data, e.g. BSRs
        # Returns None or a dictionarty containing the "includedData"
        url, marketplace_id, region = self.get_marketplace_details_tuple(
            marketplace_region
        )
        url = "".join([url, "/catalog/2022-04-01/items/", asin])
        params = {
            "marketplaceIds": marketplace_id,
            "includedData": ",".join(
                [
                    "salesRanks",
                    "summaries",
                    "relationships",
                    "identifiers",
                    "attributes",
                    "dimensions",
                ]
            ),
        }
        requested_at = datetime.utcnow()
        response = requests.get(
            url,
            params=params,
            headers=self.generate_request_headers(
                url=url, method="GET", region=region, params=params
            ),
        )
        # Respect endpoint response rate limit of 2 requests per second
        # Run 1 request per second, just to be sure in case of conflicts
        time.sleep(
            max(
                0,
                (
                    requested_at + timedelta(seconds=1) - datetime.utcnow()
                ).total_seconds(),
            )
        )
        if not response.status_code == 200:
            if response.json()["errors"][0]["code"] == "NOT_FOUND":
                # item wasn't found in marketplace, ignore error and return None
                return
            else:
                raise Exception(f"Error {response.status_code}: {response.text}")
        return {f"catalogue_{key}": value for key, value in response.json().items()}
