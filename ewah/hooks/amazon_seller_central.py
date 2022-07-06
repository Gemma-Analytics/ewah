"""
Airbyte's implementation of this connector has been an important source of information
in the process of building this EWAH connector. Check out Airbyte for more info!
"""

from ewah.hooks.base import EWAHBaseHook

from datetime import datetime, date, timedelta
from dateutil.parser import parse as parse_datetime
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


class EWAHAmazonSellerCentralHook(EWAHBaseHook):

    # Allowed reports with the alias and various settings
    _REPORT_METADATA = {
        "orders": {
            "report_type": "GET_XML_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL",
            "report_options": {},
            "method_name": "get_order_data_from_reporting_api",
            "primary_key": ["AmazonOrderID"],
            "subsequent_field": "LastUpdatedDate",
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
        },
    }

    _APIS = {
        "orders": {
            "path": "/orders/v0/orders",
            "default_rate": 0.0055,
            "datetime_filter_params": ["CreatedAfter", "LastUpdatedAfter"],
            "datetime_filter_field": "LastUpdateDate",
            "primary_key": "AmazonOrderId",
            "payload_field": "Orders",
            "sideloads": ["orderitems"],
        },
        "orderitems": {  # only for use as sideload
            "path": "/orders/v0/orders/{id}/orderItems",
            "default_rate": 0.0055,
            "payload_field": "OrderItems",
        },
    }

    _THROTTLING_DICT = {}

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
                "AWS Access Key ID", widget=BS3TextFieldWidget()
            ),
            "extra__ewah_amazon_seller_central__aws_secret_access_key": StringField(
                "AWS Secret Access Key", widget=BS3PasswordFieldWidget()
            ),
            "extra__ewah_amazon_seller_central__aws_arn_role": StringField(
                "AWS ARN - Role", widget=BS3TextFieldWidget()
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
                if key.endswith("Date"):
                    row[key] = parse_datetime(value)
            return row

        return string_to_date

    @classmethod
    def validate_marketplace_region(cls, marketplace_region, raise_on_failure=False):
        # Validate whether a region exists. Return boolean or raise exception.
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
            if isinstance(data_from, datetime):
                data_from = data_from.date()
            if isinstance(data_from, date):
                data_from = data_from.isoformat()
            body["dataStartTime"] = data_from

        if data_until:
            if isinstance(data_until, datetime):
                data_until = data_until.date()
            if isinstance(data_until, date):
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
            sleep_for = sleep_base * (2 ** tries)
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
        assert document_response.status_code == 200, document_response.text
        if is_gzip:
            document_content = gzip.decompress(document_response.content)
        else:
            document_content = document_response.content

        return document_content.decode()

    def get_order_data_from_reporting_api(
        self,
        marketplace_region,
        report_name,
        data_from,
        data_until,
        report_options=None,
        batch_size=10000,
    ):
        def simple_xml_to_json(xml):
            # Takes xml and turns it to a (nested) dictionary
            response = {}
            for child in list(xml):
                if len(list(child)) > 0:
                    if response.get(child.tag):
                        if isinstance(response[child.tag], dict):
                            # tag exists more than once
                            # -> turn into a list of dicts
                            response[child.tag] = [response[child.tag]]
                        # tag exists and is already a list - append
                        response[child.tag].append(simple_xml_to_json(child))
                    else:
                        # tag does not yet exist, add it
                        response[child.tag] = simple_xml_to_json(child)
                else:
                    # tag is a scalar, add it
                    response[child.tag] = child.text
            return response

        data_string = self.get_report_data(
            marketplace_region,
            report_name,
            data_from,
            data_until,
            report_options,
        )
        if not data_string:
            # No data to provide
            yield []
        raw_data = simple_xml_to_json(ET.fromstring(data_string))["Message"]
        data = []
        i = 0
        while raw_data:
            # Improve data format and respect batch size
            i += 1
            data.append(raw_data.pop(0)["Order"])
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
        batch_size=10000,  # is ignored in this specific function
    ):
        delta_day = timedelta(days=1)
        while True:
            # sales and traffic report needs to be requested individually per day
            started = datetime.now()  # used at the end of the loop
            data_from_dt = data_from
            if isinstance(data_from_dt, pendulum.DateTime):
                # Uploader class doesn't like Pendulum (data_from_dt is added to data)
                data_from_dt = datetime.fromtimestamp(
                    data_from_dt.timestamp(), pendulum.tz.UTC
                )
            data_raw = self.get_report_data(
                marketplace_region, report_name, data_from, data_from, report_options
            )
            data = json.loads(data_raw)["salesAndTrafficByAsin"]
            for datum in data:
                # add the requested day to all rows
                datum["date"] = data_from_dt
            yield data
            data_from += delta_day
            if data_from > data_until:
                break
            self.log.info("Delaying execution for up to a minute...")
            # Delaying to avoid hitting API request rate limits
            # Wait until 1 minute after the start of the loop (hence the started var)
            time.sleep(
                max(
                    0,
                    (started + timedelta(seconds=60) - datetime.now()).total_seconds(),
                )
            )
            started = datetime.now()

    def get_data_from_reporting_api_in_batches(
        self,
        marketplace_region,
        report_name,
        data_from=None,
        data_until=None,
        report_options=None,
        batch_size=10000,
    ):
        error_msg = """Invalid report name {1}! Valid options:
        \n\t- {0}
        """.format(
            "\n\t- ".join(self._REPORT_METADATA.keys()),
            report_name,
        )
        assert report_name in self._REPORT_METADATA.keys(), error_msg
        method = getattr(self, self._REPORT_METADATA[report_name]["method_name"])
        data = []
        for batch in method(
            marketplace_region, report_name, data_from, data_until, report_options
        ):
            data += batch
            if len(data) >= batch_size:
                yield data
                data = []
        if data:
            # Last batch may otherwise not be yielded if below threshold of batch_size
            yield data

    def make_api_call_and_return_payload(
        self,
        resource,
        path,
        marketplace_region,
        since_date=None,
        since_type=None,
        next_token=None,
        logging=True,
        additional_api_call_params=None,
    ):
        # TODO: use generate_request_headers method in here

        if self._THROTTLING_DICT.get(resource):
            # APIs use token bucket-style requests throttling
            next_call_after = self._THROTTLING_DICT[resource]
            if logging:
                self.log.info(
                    f"Waiting for next call until {next_call_after.isoformat()}..."
                )
            sleeptime = next_call_after - pendulum.now()
            time.sleep(max(0, sleeptime.microseconds / 1000000 + sleeptime.seconds))

        current_ts = pendulum.now("utc")  # as late as possible -> set after waiting
        amz_date = current_ts.strftime("%Y%m%dT%H%M%SZ")
        datestamp = current_ts.strftime("%Y%m%d")

        endpoint, marketplace_id, region = self.get_marketplace_details_tuple(
            marketplace_region
        )
        metadata = self._APIS[resource]
        if not path.startswith("/"):
            path = "/" + path
        if since_date:
            if since_type == "CreatedAfter":
                filter_field = metadata["datetime_filter_params"][0]
            elif since_type == "LastUpdatedAfter":
                filter_field = metadata["datetime_filter_params"][1]
            else:
                raise Exception("This exception ought to have been caught earlier.")

        url = "".join([endpoint, path])

        params = copy.deepcopy(additional_api_call_params or {})
        params["MarketplaceIds"] = marketplace_id
        params["MaxResultsPerPage"] = 100

        if since_date:
            params[filter_field] = (
                since_date.replace(microsecond=0)
                .astimezone(pytz.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            if not next_token and logging:
                self.log.info(
                    f"Loading data from {filter_field}: {params[filter_field]}..."
                )

        if next_token:
            params["NextToken"] = next_token

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
        payload_hash = hashlib.sha256("".encode("utf-8")).hexdigest()
        canonical_request = "\n".join(
            [
                "GET",
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

        headers = {
            "content-type": "application/json",
            "host": host,
            "user-agent": "python-requests",
            "x-amz-access-token": self.access_token,
            "x-amz-date": amz_date,
            "x-amz-security-token": self.aws_session_token,
            "authorization": authorization_header,
        }

        response = requests.get(url, params=params, headers=headers)
        assert response.status_code == 200, response.text

        self._THROTTLING_DICT[resource] = pendulum.now().add(
            seconds=(
                1
                / float(
                    response.headers.get(
                        "x-amzn-RateLimit-Limit",
                        metadata.get("default_rate", 0.0055),
                    )
                )
            )
        )
        return response.json()["payload"]

    def get_data_in_batches(
        self,
        resource,
        marketplace_region,
        path=None,
        since_date=None,
        since_type=None,
        sideloads=None,
        logging=True,
        additional_api_call_params=None,
    ):
        next_token = None
        metadata = self._APIS[resource]
        path = path or metadata["path"]
        if since_date:
            assert since_type in ("CreatedAfter", "LastUpdatedAfter")
        else:
            assert since_type is None

        while True:
            if logging:
                self.log.info("Making new request...")
            response_payload = self.make_api_call_and_return_payload(
                resource,
                path,
                marketplace_region,
                since_date,
                since_type,
                next_token,
                logging,
                additional_api_call_params,
            )
            next_token = response_payload.get("NextToken")

            data = response_payload.pop(metadata["payload_field"], None)
            if data:
                if sideloads:
                    # sideloads are API calls that need to be made per row of original
                    # result. E.g. orderitems for each order. This requires a loop
                    # over each original row and one request for each row.
                    if logging:
                        self.log.info(
                            f"Loading sideloads ({(', '.join(sideloads))}) for "
                            f"{len(data)} rows of {resource}."
                        )
                    for datum in data:
                        id = datum[metadata["primary_key"]]
                        for sideload in sideloads:
                            # sideloads have no "since_date"
                            sideload_data = []
                            sideload_meta = self._APIS[sideload]
                            for batch in self.get_data_in_batches(
                                resource=sideload,
                                path=sideload_meta["path"].format(id=id),
                                marketplace_region=marketplace_region,
                                logging=False,
                            ):
                                if batch:
                                    sideload_data += batch
                            datum["sideload_" + sideload] = sideload_data
                yield data
            elif next_token:
                raise Exception(
                    "No data was returned, but a NextToken -> something went wrong!"
                )
            if not next_token:
                # We've reached the last page
                break
