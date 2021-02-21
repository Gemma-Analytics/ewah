from ewah.hooks.base import EWAHBaseHook

import requests


class EWAHAirflowHook(EWAHBaseHook):
    """Get Airflow Metadata from an Airflow installation via the stable API."""

    _ATTR_RELABEL = {
        "url": "host",
        "user": "login",
    }

    conn_name_attr = "airflow_conn_id"
    default_conn_name = "airflow_default"
    conn_type = "ewah_airflow"
    hook_name = "EWAH Airflow Connection"

    # Resolve some more complex endpoints
    _ENDPOINTS = {
        "dagRuns": "dags/~/dagRuns",
        "taskInstance": "dags/~/dagRuns/~/taskInstances",
    }

    _BASE_URL = "{0}/api/v1/{1}"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra"],
            "relabeling": {
                "host": "URL (without endpoint, e.g. 'http://myairflowurl.com')",
                "login": "Basic Auth Username",
                "password": "Baisc Auth Password",
            },
        }

    def get_data_in_batches(self, endpoint, page_size=100, batch_size=10000):
        auth = requests.auth.HTTPBasicAuth(self.conn.login, self.conn.password)
        url = self._BASE_URL.format(
            self.conn.host, self._ENDPOINTS.get(endpoint, endpoint)
        )
        params = {"limit": page_size, "offset": 0}
        data = []
        i = 0
        while True:
            i += 1
            self.log.info("Making request {0} to {1}...".format(i, url))
            request = requests.get(url, params=params, auth=auth)
            assert request.status_code == 200, request.text
            response = request.json()
            keys = list(response.keys())
            if "total_entries" in keys:
                # Most endpoint use pagination + give "total_entries" for requests
                # The key to get the data from the response may differ from endpoint
                if keys[0] == "total_entries":
                    data_key = keys[1]
                else:
                    data_key = keys[0]
                data += response[data_key]
                if len(data) >= batch_size:
                    yield data
                    data = []
                if params["offset"] >= response["total_entries"]:
                    if data:
                        yield data
                        data = []
                    break
            else:
                # Rare endpoint that does not paginate (usually singletons)
                yield [response]
                break
            params["offset"] = params["offset"] + params["limit"]
