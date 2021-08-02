from ewah.hooks.base import EWAHBaseHook

import requests
import json
import urllib
import time

from typing import List, Optional, Dict, Any
from collections import defaultdict
from time import sleep


class EWAHHubspotHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "api_key": "password",
    }

    conn_name_attr = "ewah_hubspot_conn_id"
    default_conn_name = "ewah_hubspot_default"
    conn_type = "ewah_hubspot"
    hook_name = "EWAH Hubspot Connection"

    BASE_URL = "https://api.hubapi.com/crm/v3/objects/{0}"
    PROPERTIES_URL = "https://api.hubapi.com/crm/v3/properties/{0}"
    PIPELINES_URL = "https://api.hubapi.com/crm/v3/pipelines/{0}"
    ASSOC_URL = "https://api.hubapi.com/crm/v3/associations/{fromObjectType}/{toObjectType}/batch/read"
    OWNERS_URL = "https://api.hubapi.com/crm/v3/owners/"

    ACCEPTED_OBJECTS = [
        "companies",
        "contacts",
        "deals",
        "feedback_submissions",
        "line_items",
        "products",
        "tickets",
        "quotes",
        "properties",
        "owners",
        "pipelines",
        # special cases - see get_data_in_batches function
        "engagement",
        "engagements",
        "activity",
        "activities",
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
        if object == "properties":
            return []
        request = requests.get(
            url=self.PROPERTIES_URL.format(object),
            params={"hapikey": self.conn.api_key},
            headers={"accept": "application/json"},
        )
        assert request.status_code == 200, request.text
        return [property["name"] for property in request.json()["results"]]

    def retry_request(
        self,
        url: str,
        params: dict,
        headers: Optional[dict] = None,
        expected_status_code: int = 200,
        retries: int = 3,
        wait_for_seconds: int = 60,
    ):
        # Every once in a while, the HubSpot API returns a 502 Bad Gateway
        # error. This appears to be random and related to HubSpot's server
        # infrastructure. To avoid failing DAGs due to this error, try
        # again in case of errors, but never more than 3 times.
        try_number = 0
        while try_number < retries:
            try_number += 1
            request = requests.get(url, params=params, headers=headers)
            if request.status_code == expected_status_code:
                break
            self.log.info(
                "Status {0} - Waiting {2}s and trying again. Response:\n\n{1}".format(
                    request.status_code, request.text, wait_for_seconds
                )
            )
            sleep(60)
        return request

    def get_engagements_in_batches(self, batch_size: int):
        url = "https://api.hubapi.com/engagements/v1/engagements/paged"
        limit = 250
        params = {
            "hapikey": self.conn.api_key,
            "limit": limit,
        }
        data = []
        i = 0
        while True:
            # paginate
            i += 1
            self.log.info("Getting page {0} of data...".format(str(i)))
            request = self.retry_request(
                url=url, params=params, expected_status_code=200, retries=3
            )
            assert request.status_code == 200, "\nStatus: {0}\nResponse: {1}".format(
                request.status_code, request.text
            )
            response = request.json()
            result = response.pop("results")
            while result:
                # column id is expected as primary key!
                datum = result.pop(0)
                datum["id"] = datum["engagement"]["id"]
                data.append(datum)
            if response.get("hasMore"):
                params["offset"] = response["offset"]
                if len(data) >= batch_size:
                    yield data
                    data = []
            else:
                break
        if data:
            yield data

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

        if object in ("properties", "pipelines"):
            # Special case: not a normal object
            assert not associations
            assert not properties
            assert not exclude_properties

            if object == "properties":
                url_object_raw = self.PROPERTIES_URL
                object_list = [  # engagements is OK! but only get them once
                    o
                    for o in self.ACCEPTED_OBJECTS
                    if not o
                    in (
                        "properties",
                        "owners",
                        "engagement",
                        "activity",
                        "activities",
                        "pipelines",
                    )
                ]
            elif object == "pipelines":
                url_object_raw = self.PIPELINES_URL
                object_list = ["tickets", "deals"]

            params_object["objectType"] = object_list.pop(0)
            url_object = url_object_raw.format(params_object["objectType"])
        elif object in ("engagement", "engagements", "activity", "activities"):
            # As of 2021-07-28, engagements are not part of the v3 of the API.
            # Thus, they need special treatment.
            # Also a special case and not a normal object.
            assert not associations
            assert not properties
            assert not exclude_properties
            for batch in self.get_engagements_in_batches(batch_size):
                yield batch
            return
        elif object == "owners":
            assert not associations
            assert not properties
            assert not exclude_properties
            url_object = self.OWNERS_URL
        else:
            url_object = self.BASE_URL.format(object)
            properties = [
                property
                for property in (properties or self.get_properties_for_object(object))
                if not property in (exclude_properties or [])
            ]
            params_object["properties"] = properties

            self.log.info(
                "Loading these properties:\n\n\t- {0}\n\n".format(
                    "\n\t- ".join(properties)
                )
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
            request = self.retry_request(
                url=url_object,
                params=params_object,
                headers={"accept": "application/json"},
                expected_status_code=200,
                retries=3,
            )
            if request.status_code == 414:
                _msg = (
                    "Error: Too many properties. Please use a smaller, custom set of"
                    " properties.\n\nUsed properties in this call:\n\t"
                )
                _msg += "\n\t".join(properties) + "\n\n"
                raise Exception(_msg)
            assert request.status_code == 200, "Status {0}: {1}".format(
                request.status_code, request.text
            )
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
            # 3) if getting properties or pipelines: add object metadata
            for datum in response_data:
                datum.update(datum.pop("properties", {}))  # 1)
                for association in associations or []:  # 2)
                    datum[
                        "ewah_associations_to_{0}".format(association)
                    ] = associations_data[association].get(datum["id"])
                if object in ("properties", "pipelines"):  # 3)
                    datum["object_type"] = params_object["objectType"]

            # batch_data saves all data until it is yielded
            batch_data += response_data

            if not keepgoing and object in ("properties", "pipelines"):
                # Iterate through list of all objects
                if object_list:
                    params_object["objectType"] = object_list.pop(0)
                    # tbd
                    url_object = url_object_raw.format(params_object["objectType"])
                    keepgoing = True

            # Yield data when appropriate
            if (len(batch_data) >= batch_size) or (not keepgoing and batch_data):
                yield batch_data
                batch_data = []
