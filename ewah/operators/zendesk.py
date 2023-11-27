from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.base import EWAHBaseHook as BaseHook

from datetime import datetime, timedelta

from requests.auth import HTTPBasicAuth
import requests
import json
import time


class EWAHZendeskOperator(EWAHBaseOperator):
    _NAMES = ["zendesk"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: True,
    }

    _base_url = "https://{support_url}.zendesk.com/{endpoint}"

    def __init__(self, support_url, resource, auth_type, *args, **kwargs):
        self._accepted_resources = {
            "tickets": {
                "function": self.execute_incremental_cursor_based,
                "datetime_field": "updated_at",
            },
            "ticket_audits": {
                "function": self.execute_incremental_cursor_based,
                "datetime_field": "created_at",
            },
            "ticket_metric_events": {
                "function": self.execute_incremental_time_based,
            },
            "users": {
                "function": self.execute_incremental_time_based,
            },
            "ticket_fields": {
                "function": self.get_custom_ticket_fields,
                "drop_and_replace": True,
            },
        }

        if not resource in self._accepted_resources.keys():
            raise Exception('resource "{0}" not supported!'.format(resource))

        if not auth_type in ["basic_auth"]:
            raise Exception("auth_type must be basic_auth!")

        kwargs["primary_key"] = kwargs.get("primary_key", "id")

        if self._accepted_resources[resource].get("drop_and_replace"):
            kwargs["extract_strategy"] = EC.ES_FULL_REFRESH

        # API data is delayed by about 60s according to official docs
        kwargs["wait_for_seconds"] = max(kwargs.get("wait_for_seconds", 0), 70)

        super().__init__(*args, **kwargs)

        self.support_url = support_url
        self.resource = resource
        self.auth_type = auth_type

    def ewah_execute(self, context):
        self.z_data_from = self.make_unix_datetime(self.data_from)
        self.z_data_until = self.make_unix_datetime(self.data_until)

        conn = self.source_conn
        if self.auth_type == "basic_auth":
            self.auth = HTTPBasicAuth(conn.login, conn.password)
        else:
            raise Exception("auth_type not implemented!")

        self._metadata.update({"support_url": self.support_url})

        # run correct execute function
        return self._accepted_resources[self.resource]["function"](context, self)

    def make_unix_datetime(self, dt):
        return str(int(time.mktime(dt.timetuple())))

    def get_custom_ticket_fields(_, context, self):
        url = self._base_url.format(
            support_url=self.support_url,
            endpoint="api/v2/ticket_fields.json",
        )
        req = requests.get(url, auth=self.auth)
        if not req.status_code == 200:
            raise Exception(
                "Error {1} when calling Zendesk API: {0}".format(
                    req.text,
                    str(req.status_code),
                )
            )
        data = json.loads(req.text)["ticket_fields"]
        self.upload_data(data)

    def execute_incremental_cursor_based(_, context, self):
        if not self.resource in ["tickets", "ticket_audits"]:
            raise Exception("cursor-based load not implemented!")

        if self.resource == "tickets":
            endpoint = "api/v2/incremental/tickets/cursor.json"
            params = {
                "start_time": self.z_data_from,
                "include": "metric_sets",
            }
            response_resource = "tickets"
        elif self.resource == "ticket_audits":
            endpoint = "api/v2/ticket_audits.json"
            params = {
                "start_time": self.z_data_from,
            }
            response_resource = "audits"

        datetime_field = self._accepted_resources[self.resource]
        datetime_field = datetime_field["datetime_field"]

        url = self._base_url.format(
            support_url=self.support_url,
            endpoint=endpoint,
        )

        first_call = True
        while first_call or req.status_code == 200:
            first_call = False

            self.log.info("Requesting a page of data...")
            req = requests.get(url, params=params, auth=self.auth)
            response = json.loads(req.text)
            if req.status_code == 200:
                data = response[response_resource]
                if data:
                    last_updated_at = datetime.strptime(
                        data[-1][datetime_field],
                        "%Y-%m-%dT%H:%M:%SZ",
                    )
                    self.upload_data(data)
                if (
                    response.get("end_of_stream")
                    or last_updated_at > self.load_data_until
                ):
                    break
                params = {"cursor": response.get("after_cursor")}

        if not req.status_code == 200:
            raise Exception(
                "Error {1} when calling Zendesk API: {0}".format(
                    req.text,
                    str(req.status_code),
                )
            )

    def execute_incremental_time_based(_, context, self):
        url = self._base_url.format(
            **{
                "support_url": self.support_url,
                "endpoint": "api/v2/incremental/{0}.json".format(self.resource),
            }
        )

        params = {
            "start_time": self.z_data_from,
        }

        # Time-based incremental exports, run the first, the paginate!
        self.log.info(
            "Calling Zendesk api.\nurl: {0}\nparams:{1}".format(
                url,
                str(params),
            )
        )
        r = requests.get(url, params=params, auth=self.auth)
        data = json.loads(r.text)
        while (
            r.status_code == 200
            and not data.get("end_of_stream")
            and data.get("end_time")
            and data.get("end_time") <= int(self.z_data_until)
        ):
            self.upload_data(data[self.resource])  # uploads previous request
            self.log.info("Requesting next page of data...")
            r = requests.get(data["next_page"], auth=self.auth)  # new request
            data = json.loads(r.text)

        if not r.status_code == 200:
            raise Exception(
                "Error {1} when calling Zendesk API: {0}".format(
                    r.text,
                    str(r.status_code),
                )
            )

        # upload final request
        self.upload_data(data[self.resource])
