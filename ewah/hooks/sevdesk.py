from ewah.hooks.base import EWAHBaseHook

import requests


class EWAHSevDeskHook(EWAHBaseHook):
    conn_name_attr = "ewah_sevdesk_conn_id"
    default_conn_name = "ewah_sevdesk_default"
    conn_type = "ewah_sevdesk"
    hook_name = "EWAH SevDesk Connection"

    _ENDPOINTS = [
        "CheckAccount",
        "CheckAccountTransaction",
        "CommunicationWay",
        "Contact",
        "Invoice",
        "InvoicePos",
        "Order",
        "OrderPos",
        "Part",
        "Voucher",
        "VoucherPos",
    ]
    _BASE_URL = "https://my.sevdesk.de/api/v1/"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "host", "login"],
            "relabeling": {
                "password": "API Token",
            },
        }

    @classmethod
    def validate_endpoint(cls, endpoint):
        return endpoint in cls._ENDPOINTS

    def get_data_in_batches(self, endpoint, batch_size=10000, embed=None):
        assert self.validate_endpoint(endpoint), "Invalid endpoint '{0}'!".format(
            endpoint
        )
        url = self._BASE_URL + endpoint
        params = {
            "limit": batch_size,
            "offset": 0,
            "token": self.conn.password,
        }
        if embed:
            if not isinstance(embed, str):
                # Cast iterable into CSV
                embed = ",".join(embed)
            params["embed"] = embed
        self.log.info("Request URL: {0}".format(url))
        while True:
            self.log.info("Requesting a new page...")
            request = requests.get(url, params=params)
            assert request.status_code == 200, request.text
            response = request.json()["objects"]
            if response:
                params["offset"] = params["offset"] + len(response)
                yield response
            else:
                # We're done
                break
