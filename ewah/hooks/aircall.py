from ewah.hooks.base import EWAHBaseHook

import requests
import time


class EWAHAircallHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "api_id": "login",
        "api_token": "password",
    }

    conn_name_attr = "aircall_conn_id"
    default_conn_name = "aircall_default"
    conn_type = "ewah_aircall"
    hook_name = "EWAH Aircall Connection"

    _RESOURCES = {
        "users": {"incremental": True},
        "teams": {},
        "calls": {"incremental": True},
        "numbers": {"incremental": True},
        "contacts": {"incremental": True},
        "tags": {},
    }
    _BASE_URL = "https://api.aircall.io/v1/{0}"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "host"],
            "relabeling": {
                "login": "Basic Auth API ID",
                "password": "Baisc Auth API Token",
            },
        }

    def get_data_in_batches(
        self,
        resource,
        data_from=None,
        data_until=None,
        batch_size=10000,
        batch_call_pause_seconds=1,
    ):
        _msg = "batch_size param must be a positive integer <= 10k "
        assert isinstance(batch_size, int), _msg
        assert batch_size > 0, _msg
        assert batch_size <= 10000, _msg

        page_size = 50  # maximum page size is 50

        auth = requests.auth.HTTPBasicAuth(
            self.conn.api_id,
            self.conn.api_token,
        )
        url = self._BASE_URL.format(resource)
        params = {
            "per_page": page_size,
        }
        if data_from:
            params["from"] = int(time.mktime(data_from.timetuple()))
        if data_until:
            params["to"] = int(time.mktime((data_until).timetuple()))

        data = []
        while url:
            time.sleep(batch_call_pause_seconds)
            request = requests.get(url, params=params, auth=auth)
            assert request.status_code == 200, request.text
            response = request.json()
            url = response.get("meta", {}).get("next_page_link")
            data += response.get(resource, [])
            if (not url) or (len(data) + page_size > batch_size):
                yield data
                data = []
