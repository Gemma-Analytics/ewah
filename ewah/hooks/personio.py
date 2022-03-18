from ewah.hooks.base import EWAHBaseHook

import requests

from datetime import datetime, date


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

    def get_data_in_batches(self, resource, data_from=None):
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
            # These params are required. Just make them ridiculous.
            params["start_date"] = "1900-01-01"
            params["end_date"] = "2100-01-01"
            if data_from:
                if isinstance(data_from, (date, datetime)):
                    params["updated_from"] = data_from.isoformat()
                else:
                    params["updated_from"] = data_from
        else:
            assert not data_from, "data_from is only valid for attendances!"
        if resource in ["absences", "time-offs"]:
            # It appears as if the "limit" parameter is used like a "page"
            # parameter in Personio's API for absences. Hence, start with one,
            # and incremental like a serial.
            params["offset"] = 1

        while True:
            self.log.info("Requesting a page of data...")
            # Token needs to be re-requested for every API call!
            headers["Authorization"] = "Bearer {0}".format(self.token)
            response = requests.get(url, params=params, headers=headers)
            assert response.status_code == 200, response.text
            response_data = response.json()
            assert response_data.get("success"), response_data
            if response_data.get("data"):
                # Unpack attributes dict
                # Format of dict differs across endpoints! Cover all of them
                data = response_data.pop("data")
                for datum in data:
                    attributes = datum.pop("attributes", {})
                    for attribute, value in attributes.items():
                        if resource == "employees":
                            value = value["value"]
                        if (
                            resource in ["absences", "time-offs"]
                            and attribute == "employee"
                        ):
                            # Don't pull PII from absences, only employee ID!
                            # If able, that data can be pulled from the employees
                            # endpoint!
                            value = value["attributes"]["id"]["value"]
                        datum[attribute] = value
                data_len = len(data)
                yield data
            else:
                break
            if (  # don't use the page parameter - doesn't work properly!
                response_data.get("limit") and data_len < int(response_data["limit"])
            ) or (not response_data.get("limit")):
                # If there's not limit parameter, then there's no pagination at all
                # We're done here
                break
            if resource in ["absences", "time-offs"]:
                # As discussed above: in the case of absences/time-offs (same endpoint),
                # the "offset" parameter appears to be used like a "page" parameter.
                # This is a workaround to work with this buggy API behavior.
                params["offset"] = params["offset"] + 1
            else:
                params["offset"] = int(response_data["offset"]) + int(
                    response_data["limit"]
                )
