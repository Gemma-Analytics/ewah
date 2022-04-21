"""
Airbyte's implementation of this connector has been an important source of information
in the process of building this EWAH connector. Check out Airbyte for more info!
"""

from ewah.hooks.base import EWAHBaseHook

from datetime import datetime
import urllib.parse
import pendulum
import hashlib
import hmac
import boto3
import requests
import time
import pytz


class EWAHAmazonSellerCentralHook(EWAHBaseHook):

    _APIS = {
        "orders": {
            "path": "/orders/v0/orders",
            "default_rate": 0.0055,
            "datetime_filter_param": "LastUpdatedAfter",
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
                    row[key] = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=pytz.utc
                    )
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
                seconds=response_data["expires_in"]
            )
        return self._access_token

    @property
    def boto3_role_credentials(self):
        if not hasattr(self, "_boto3_role_credentials"):
            self.log.info("Logging into AWS...")
            boto3_client = boto3.client(
                "sts",
                aws_access_key_id=self.conn.aws_access_key_id,
                aws_secret_access_key=self.conn.aws_secret_access_key,
            )
            role = boto3_client.assume_role(
                RoleArn=self.conn.aws_arn_role, RoleSessionName="guid"
            )
            self._boto3_role_credentials = role["Credentials"]
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

    def make_api_call_and_return_payload(
        self,
        resource,
        path,
        marketplace_region,
        since_date=None,
        next_token=None,
        logging=True,
    ):

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
            filter_field = metadata["datetime_filter_param"]
        url = "".join([endpoint, path])
        params = {
            "MarketplaceIds": marketplace_id,
            "MaxResultsPerPage": 100,
        }
        if since_date:
            params[filter_field] = (
                since_date.replace(microsecond=0)
                .astimezone(pytz.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            if not next_token and logging:
                self.log.info(f"Loading data from {params[filter_field]}...")
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
        sideloads=None,
        logging=True,
    ):
        next_token = None
        metadata = self._APIS[resource]
        path = path or metadata["path"]
        while True:
            if logging:
                self.log.info("Making new request...")
            response_payload = self.make_api_call_and_return_payload(
                resource, path, marketplace_region, since_date, next_token, logging
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
                            f"Loading sideloads ({(', '.join(sideloads))}) for {len(data)} rows of {resource}."
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
