from ewah.hooks.base import EWAHBaseHook

from typing import List, Dict, Any, Optional

import requests
import time
import copy


class EWAHPipedriveHook(EWAHBaseHook):

    _ATTR_RELABEL: dict = {
        "company": "login",
        "api_token": "password",
    }

    conn_name_attr: str = "ewah_pipedrive_conn_id"
    default_conn_name: str = "ewah_pipedrive_default"
    conn_type: str = "ewah_pipedrive"
    hook_name: str = "EWAH Pipedrive Connection"

    _URL: str = "https://{company}.pipedrive.com/api/v1/{endpoint}"
    _REQUESTS_LEFT = "x-ratelimit-remaining"

    def get_data_in_batches(
        self, object: str, batch_size: int = 10000, **kwargs
    ) -> List[Dict[str, Any]]:
        params = copy.deepcopy(kwargs)
        # Pagination
        params["start"] = 0
        params["limit"] = 500
        # Authentication
        params["api_token"] = self.conn.api_token

        url = self._URL.format(company=self.conn.company, endpoint=object)

        first_call = True
        data = []
        while first_call or (success and params["start"]):
            first_call = False
            request = requests.get(url, params=params)
            result = request.json()
            success = request.status_code == 200 and result.get("success")
            if success:
                if int(request.headers[self._REQUESTS_LEFT]) < 10:
                    # Wait for ratelimit to fill up again
                    time.sleep(2)

                data += result["data"]
                if (
                    result.get("additional_data", {})
                    .get("pagination", {})
                    .get("next_start")
                ):
                    params["start"] = result["additional_data"]["pagination"][
                        "next_start"
                    ]
                else:
                    # Stop iterating, no more pages to go
                    params["start"] = None

                if data and ((params["start"] is None) or (len(data) >= batch_size)):
                    yield data
                    data = []

        if not success:
            raise Exception("Error - request response: {0}".format(request.text))

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["port", "extra", "host", "schema"],
            "relabeling": {
                "password": "API Token",
                "login": "Company (from the URL {company}.pipedrive.com)",
            },
        }
