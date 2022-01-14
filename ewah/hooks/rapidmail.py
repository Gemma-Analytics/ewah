from ewah.hooks.base import EWAHBaseHook

from typing import List, Dict, Any, Optional

import requests


class EWAHRapidmailHook(EWAHBaseHook):

    _ATTR_RELABEL: {}

    conn_name_attr = "ewah_rapidmail_conn_id"
    default_conn_name = "ewah_rapidmail_default"
    conn_type = "ewah_rapidmail"
    hook_name = "EWAH Google Ads Connection"

    URL = "https://apiv3.emailsys.net"
    ALLOWED_ENDPOINTS = (
        "apiusers",
        "blacklist",
        "forms",
        "mailings",
        "recipientlists",
        "recipients",
        "trx/emails",
    )

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "host", "port", "schema"],
            "relabeling": {
                "login": "API User ID",
                "password": "API User Password",
            },
        }

    def get_all_data(self, endpoint: str) -> List[Dict[str, Any]]:
        assert endpoint in self.ALLOWED_ENDPOINTS, "Invalid endpoint '{0}'!".format(
            endpoint
        )
        # special case: recipients can only be fetched by recipientlist_id
        if endpoint == "recipients":
            recipient_lists = self.get_data(endpoint="recipientlists")
            for recipient_list in recipient_lists:
                params = {"recipientlist_id": recipient_list["id"]}
                yield self.get_data(endpoint=endpoint, params=params)
        else:
            yield self.get_data(endpoint=endpoint)

    def get_data(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        data = []
        page = 1
        params = params or {}

        auth = requests.auth.HTTPBasicAuth(self.conn.login, self.conn.password)

        while True:
            self.log.info("Requesting page {0}...".format(page))
            params["page"] = page
            request = requests.get(
                "/".join((self.URL, endpoint)), auth=auth, params=params
            )
            assert request.status_code == 200, request.text
            response = request.json()
            data += response["_embedded"][endpoint.replace("/", "")]
            if page == 1:
                self.log.info(
                    "Total pages: {0}".format(
                        response["page_count"]
                        or 1  # If no data exists, page_count is 0
                    )
                )
            if response["page"] == response["page_count"]:
                # we're done here
                break
            page += 1

        return data
