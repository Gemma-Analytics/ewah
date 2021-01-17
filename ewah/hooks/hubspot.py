from ewah.hooks.base import EWAHBaseHook

import requests
import json
import urllib
import time

from typing import List, Optional, Dict, Any
from collections import defaultdict


class EWAHHubspotHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "api_key": "password",
    }

    conn_name_attr = "hubspot_conn_id"
    default_conn_name = "hubspot_default"
    conn_type = "ewah_hubspot"
    hook_name = "EWAH Hubspot Connection"

    BASE_URL = "https://api.hubapi.com/crm/v3/objects/{0}"
    PROPERTIES_URL = "https://api.hubapi.com/crm/v3/properties/{0}"
    ASSOC_URL = "https://api.hubapi.com/crm/v3/associations/{fromObjectType}/{toObjectType}/batch/read"

    ACCEPTED_OBJECTS = [
        "companies",
        "contacts",
        "deals",
        "feedback_submissions",
        "line_items",
        "products",
        "tickets",
        "quotes",
    ]

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "host", "login"],
            "relabeling": {
                "password": "API Key",
            },
        }

    def get_properties_for_object(self, object: str):
        request = requests.get(
            url=self.PROPERTIES_URL.format(object),
            params={"hapikey": self.conn.api_key},
            headers={"accept": "application/json"},
        )
        assert request.status_code == 200, request.text
        return [property["name"] for property in request.json()["results"]]

    def get_data_in_batches(
        self,
        object: str,
        properties: Optional[List[str]] = None,
        exclude_properties: Optional[List[str]] = None,
        associations: Optional[List[str]] = None,
        batch_size: int = 10000,
    ) -> List[Dict[str, Any]]:

        self.log.info("Loading data for CRM object {0}!".format(object))
        params_auth = {"hapikey": self.conn.api_key}
        params_object = {"hapikey": self.conn.api_key, "limit": 100}
        url_object = self.BASE_URL.format(object)

        properties = [
            property
            for property in (properties or self.get_properties_for_object(object))
            if not property in (exclude_properties or [])
        ]
        params_object["properties"] = properties

        self.log.info(
            "Loading these properties:\n\n\t- {0}\n\n".format("\n\t- ".join(properties))
        )
        if associations:
            self.log.info(
                "Also loading these associations:\n\n\t- {0}".format(
                    "\n\t- ".join(associations)
                )
            )

        keepgoing = True
        i = 0
        batch_data = []
        while keepgoing:
            # Get next page of object data
            time.sleep(0.2)  # Avoid hitting API rate limits
            i += 1
            self.log.info("Getting page {0} of data...".format(str(i)))
            request = requests.get(
                url=url_object,
                params=params_object,
                headers={"accept": "application/json"},
            )
            if request.status_code == 414:
                _msg = (
                    "Error: Too many properties. Please use a smaller, custom set of"
                    " properties.\n\nUsed properties in this call:\n\t"
                )
                _msg += "\n\t".join(properties) + "\n\n"
                raise Exception(_msg)
            assert request.status_code == 200, request.text
            response = request.json()
            response_data = response["results"] or []
            # Keep going as long as a link is shipped in the response
            keepgoing = response.get("paging", {}).get("next", {}).get("after")
            params_object["after"] = keepgoing

            # If applicable: get associations for all relevant objects
            if associations and response_data:
                associations_data = defaultdict(dict)
                payload = json.dumps(
                    {"inputs": [{"id": str(datum["id"])} for datum in response_data]}
                )
                for association in associations:
                    request = requests.post(
                        self.ASSOC_URL.format(
                            fromObjectType=object,
                            toObjectType=association,
                        ),
                        params=params_auth,
                        headers={
                            "accept": "application/json",
                            "content-type": "application/json",
                        },
                        data=payload,
                    )
                    assert request.status_code < 300, request.text
                    assert request.status_code >= 200, request.text
                    associations_data[association].update(
                        {
                            datum["from"]["id"]: datum["to"]
                            for datum in request.json()["results"]
                        }
                    )

            # Clean up data:
            # 1) expand the properties field into individual fields
            # 2) add any available associations
            for datum in response_data:
                datum.update(datum.pop("properties", {}))  # 1)
                for association in associations or []:  # 2)
                    datum[
                        "ewah_associations_to_{0}".format(association)
                    ] = associations_data[association].get(datum["id"])

            # batch_data saves all data until it is yielded
            batch_data += response_data

            # Yield data when appropriate
            if (len(batch_data) >= batch_size) or (not keepgoing and batch_data):
                yield batch_data
                batch_data = []
