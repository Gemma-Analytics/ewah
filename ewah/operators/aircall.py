from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from datetime import timedelta
import requests
import json
import time


class EWAHAircallOperator(EWAHBaseOperator):

    _NAMES = ["aircall"]

    _ACCEPTED_LOAD_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
    }

    _REQUIRES_COLUMNS_DEFINITION = False

    _BASE_URL = "https://api.aircall.io/v1/{0}"

    _RESOURCES = {
        "users": {"incremental": True},
        "teams": {},
        "calls": {"incremental": True},
        "numbers": {"incremental": True},
        "contacts": {"incremental": True},
        "tags": {},
    }

    def __init__(self, resource=None, wait_between_pages=1, *args, **kwargs):
        kwargs["primary_key_column_name"] = "id"
        resource = resource or kwargs.get("target_table_name")
        super().__init__(*args, **kwargs)

        assert resource in self._RESOURCES, "Invalid resource!"
        self.resource = resource
        assert isinstance(wait_between_pages, int) and wait_between_pages >= 0
        self.wait_between_pages = wait_between_pages
        if self.load_strategy == EC.ES_INCREMENTAL:
            _msg = '"{0}" cannot be loaded incrementally!'.format(resource)
            assert self._RESOURCES[resource].get("incremental"), _msg

    def _upload_aircall_data(self, url, params, auth):
        # implement pagination & upload data
        i = 0
        data = []
        while url:
            i += 1
            time.sleep(self.wait_between_pages)
            self.log.info("Requesting page {0}...".format(str(i)))
            request = requests.get(url, params=params, auth=auth)
            assert request.status_code == 200, request.text
            response = json.loads(request.text)
            url = response.get("meta", {}).get("next_page_link")
            data += response.get(self.resource, [])
            if len(data) >= 10000:
                # Multiple API endpoints have 10k limits when used without
                # the "to" and "from" params! Explicitly fail in that case!
                raise Exception("Error! 10k limit reached! Introduce chunking!")
        return self.upload_data(data)

    def ewah_execute(self, context):
        auth = requests.auth.HTTPBasicAuth(
            self.source_conn.api_id,
            self.source_conn.api_token,
        )
        url = self._BASE_URL.format(self.resource)
        params = {
            "per_page": 50,  # maximum page size is 50
        }
        if self.data_from:
            params["from"] = int(time.mktime(self.data_from.timetuple()))
        if self.data_until:
            params["to"] = int(time.mktime((self.data_until).timetuple()))
        self._upload_aircall_data(url=url, params=params, auth=auth)
