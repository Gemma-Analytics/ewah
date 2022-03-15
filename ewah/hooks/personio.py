from ewah.hooks.base import EWAHBaseHook

import requests

# from datetime import datetime
# from cryptography.fernet import Fernet


class EWAHPersonioHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "client_id": "login",
        "client_secret": "password",
    }

    conn_name_attr = "ewah_personio_conn_id"
    default_conn_name = "ewah_personio_default"
    conn_type = "ewah_personio"
    hook_name = "EWAH Personio Connection"

    BASE_URL = "https://api.personio.de/v1/"

    ENDPOINTS = {
        "employees": "company/employees",
        "absences": "company/time-offs",
        "time-offs": "company/time-offs",
        "projects": "company/attendances/projects",
        "attendances": "company/attendances",
    }

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "host"],
            "relabeling": {
                "login": "Client ID",
                "password": "Client Secret",
            },
        }

    @classmethod
    def validate_resource(cls, resource):
        assert (
            resource in cls.ENDPOINTS.keys()
        ), "'{0}' is not a valid resource! Valid resources: {1}".format(
            resource, ", ".join(cls.ENDPOINTS.keys())
        )
        return True

    @property
    def token(self):
        # Token needs to be re-requested for every API call!
        token_request = requests.post(
            self.BASE_URL + "auth",
            headers={"Accept": "application/json", "X-Personio-Partner-ID": "ewah"},
            params={
                "client_id": self.conn.client_id,
                "client_secret": self.conn.client_secret,
            },
        )
        assert token_request.status_code == 200, token_request.text
        token_data = token_request.json()
        assert token_data.get("success"), token_data
        return token_data["data"]["token"]

    def get_data_in_batches(self, resource):
        self.validate_resource(resource)
        url = self.BASE_URL + self.ENDPOINTS[resource]
        headers = {
            "Accept": "application/json",
            "X-Personio-Partner-ID": "ewah",
        }
        params = {
            "limit": 200,
            "offset": 0,
        }
        if resource == "attendances":
            params["start_date"] = "1900-01-01"
            params["end_date"] = "2100-01-01"

        while True:
            self.log.info("Requesting a page of data...")
            # Token needs to be re-requested for every API call!
            headers["Authorization"] = "Bearer {0}".format(self.token)
            response = requests.get(url, params=params, headers=headers)
            assert response.status_code == 200, response.text
            response_data = response.json()
            assert response_data.get("success"), response_data
            if response_data.get("data"):
                # Endpoints have differing ways of returning data... for now,
                # keep it simple, and just push the data the way we receive it.
                # data = [
                #     {
                #         key: value["value"] if isinstance(value, dict) else value
                #         for key, value in datum["attributes"].items()
                #     }
                #     for datum in response_data["data"]
                # ]
                data = response_data["data"]
                data_len = len(data)
                yield data
            else:
                data_len = 0
            if (
                (  # reached the last page
                    response_data.get("current_page")
                    and (response_data["current_page"] == response_data["total_pages"])
                )
                or (  # no pagination, but limit and offset shows: we're done
                    not response_data.get("current_page")
                    and response_data.get("limit")
                    and data_len < int(response_data["limit"])
                )
                or (  # there's no pagination, we're done
                    not response_data.get("current_page")
                    and not response_data.get("limit")
                )
            ):
                # We're done here
                break
            params["offset"] = response_data["offset"] + response_data["limit"]
