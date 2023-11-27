from ewah.hooks.base import EWAHBaseHook

import requests

from typing import List, Dict, Any, Optional

from datetime import datetime


class EWAHBrazeHook(EWAHBaseHook):
    _ATTR_RELABEL = {
        "api_key": "password",
        "endpoint": "host",
    }

    conn_name_attr = "ewah_braze_conn_id"
    default_conn_name = "ewah_braze_default"
    conn_type = "ewah_braze"
    hook_name = "EWAH Braze API Connection"

    OBJECTS = {
        "campaigns": {"pk": "campaign_id", "endpoint": "campaigns"},
        "canvases": {"pk": "canvas_id", "endpoint": "canvas"},
        "cards": {"pk": "card_id", "endpoint": "feed"},
    }

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "login"],
            "relabeling": {
                "password": "API Key",
                "host": "REST Endpoint",
            },
        }

    @classmethod
    def validate_resource(cls, object):
        return object in cls.OBJECTS.keys()

    @property
    def auth_header(self):
        if not hasattr(self, "_header"):
            self._header = {"Authorization": "Bearer {0}".format(self.conn.api_key)}

        return self._header

    def make_api_call(
        self, url: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        response = requests.get(url, params=params, headers=self.auth_header)
        assert response.status_code == 200, "Request Status {0}: {1}".format(
            response.status_code, response.text
        )
        return response.json()

    def get_object_list(
        self, object: str, data_from: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        url = "{0}/{1}/list".format(
            self.conn.endpoint, self.OBJECTS[object]["endpoint"]
        )
        self.log.info("Loading objects from {0} ...".format(url))
        page = 0
        params = {
            "page": page,
            "include_archived": "true",  # This needs to be a string, not a bool!
            # If this is a bool, the API will ignore the value and default to false
        }
        if data_from:
            params["last_edit.time[gt]"] = data_from.isoformat()
            self.log.info("Loading data from {0}...".format(data_from.isoformat()))
        while True:
            # pagination
            params["page"] = page
            response = self.make_api_call(url, params=params)[object]
            if response:
                yield response
                page += 1
            else:
                # pagination ends when a page is empty
                break

    def get_object_data(
        self, object: str, data_from: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        url = "{0}/{1}/details".format(
            self.conn.endpoint, self.OBJECTS[object]["endpoint"]
        )

        # for all items, get details
        for chunk in self.get_object_list(object=object, data_from=data_from):
            data = []
            while chunk:
                datum = chunk.pop(0)
                datum.update(
                    self.make_api_call(
                        url, params={self.OBJECTS[object]["pk"]: datum["id"]}
                    )
                )
                updated_at = datum.pop("updated_at", None)
                if updated_at:
                    datum["updated_at"] = datetime.strptime(
                        updated_at, "%Y-%m-%dT%H:%M:%S%z"
                    )
                data.append(datum)
            yield data
