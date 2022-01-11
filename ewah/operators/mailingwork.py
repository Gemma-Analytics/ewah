from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.base import EWAHBaseHook as BaseHook

import requests
import json
import copy


class EWAHMailingworkOperator(EWAHBaseOperator):

    _NAMES = ["mailingwork"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _BASE_URL = "https://webservice.mailingwork.de/webservice/webservice/json/"

    def __init__(
        self,
        endpoint,  # String, appended to _BASE_URL
        normal_params=None,  # any additional params other than credentials
        iter_param=None,  # a param to iterate over
        page_size=0,  # 0 means no pagination at all!
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        if not normal_params is None:
            assert isinstance(normal_params, dict), "normal_params must be dict"
        if not iter_param is None:
            _msg = """iter_param must be a dict with two key-value pairs:
            1) name: a string, naming the parameter
            2) values: a list of values, where each call will be made once with
            this value. Example: {'name': 'listId', 'values': [2, 3]}
            """
            assert isinstance(iter_param, dict), _msg
            assert isinstance(iter_param.get("name"), str), _msg
            assert isinstance(iter_param.get("values"), list), _msg
            _msg = "Must provide primary_key if using iter_param!"
            assert self.primary_key, _msg

        _msg = "page_size must be a non-negative integer!"
        assert isinstance(page_size, int), _msg
        assert page_size >= 0, _msg

        self.endpoint = endpoint
        self.normal_params = normal_params
        self.iter_param = iter_param
        self.page_size = page_size

    def ewah_execute(self, context):
        def get_mailingwork_data(url, data):
            # send request, assert success, and return the resulting data
            req = requests.post(url, data=data)
            _m = "Error {0} - Response Text: {1}"
            assert req.status_code == 200, _m.format(req.status_code, req.text)
            result = json.loads(req.text)
            assert result["error"] == 0, result["message"]
            return result["result"]

        def call_api(url, data):
            if self.page_size == 0:
                return get_mailingwork_data(url=url, data=data)
            else:
                self.log.info("Paginating request!")
                limit = self.page_size
                offset = 0
                keep_going = True
                # don't change data in calling namespace
                data = copy.deepcopy(data)
                final_result = []
                while keep_going:
                    _log = "Paginated request No. {0}\nlimit={1}, offset={2}..."
                    self.log.info(
                        _log.format(
                            str(int(1 + offset / limit)),
                            limit,
                            offset,
                        )
                    )
                    # don't get confused by the notation - do NOT supply
                    # a dict like advanced = {'limit': 100, 'start': 0}, instead
                    # supply them like below as individual key-value pairs
                    data.update(
                        {
                            "advanced[limit]": limit,
                            "advanced[start]": offset,
                        }
                    )
                    result = get_mailingwork_data(url=url, data=data)
                    if result:
                        final_result += result
                        offset += limit
                        keep_going = len(result) == limit
                    else:
                        # reached the last page
                        keep_going = False
                return final_result

        post_data = {
            "username": self.source_conn.login,
            "password": self.source_conn.password,
        }
        post_data.update(self.normal_params or {})

        url = self._BASE_URL + self.endpoint
        self.log.info("Fetching data from {0}...".format(url))
        if self.iter_param:
            param_name = self.iter_param["name"]
            for value in self.iter_param["values"]:
                self.log.info(
                    "Fetching data for {0}={1}...".format(
                        param_name,
                        value,
                    )
                )
                self._metadata.update({param_name: value})
                post_data.update({param_name: value})
                self.upload_data(call_api(url=url, data=post_data))
        else:
            self.upload_data(call_api(url=url, data=post_data))
