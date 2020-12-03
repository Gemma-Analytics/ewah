from ewah.operators.base_operator import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import requests
import json

class EWAHMailingworkOperator(EWAHBaseOperator):

    _IS_INCREMENTAL = False
    _IS_FULL_REFRESH = True

    _REQUIRES_COLUMNS_DEFINITION = False

    _BASE_URL = 'https://webservice.mailingwork.de/webservice/webservice/json/'

    def __init__(self,
        endpoint, # String, appended to _BASE_URL
        normal_params=None, # any additional params other than credentials
        iter_param=None, # a param to iterate over
    *args, **kwargs):
        if not normal_params is None:
            assert isinstance(normal_params, dict), 'normal_params must be dict'
        if not iter_param is None:
            _msg = """iter_param must be a dict with two key-value pairs:
            1) name: a string, naming the parameter
            2) values: a list of values, where each call will be made once with
            this value. Example: {'name': 'listId', 'values': [2, 3]}
            """
            assert isinstance(iter_param, dict), _msg
            assert isinstance(iter_param.get('name'), str), _msg
            assert isinstance(iter_param.get('values'), list), _msg
            _msg = 'Must add Metadata if using iter_param!'
            assert kwargs.get('add_metadata', True), _msg

        self.endpoint = endpoint
        self.normal_params = normal_params
        self.iter_param = iter_param

        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):

        def call_api(url, data):
            request = requests.post(url, data=data)
            assert request.status_code == 200, request.text
            result = json.loads(request.text)
            assert result['error'] == 0, result['message']
            return result['result']

        post_data = {
            'username': self.source_conn.login,
            'password': self.source_conn.password,
        }
        post_data.update(self.normal_params or {})

        url = self._BASE_URL + self.endpoint
        self.log.info('Fetching data from {0}...'.format(url))
        if self.iter_param:
            param_name = self.iter_param['name']
            for value in self.iter_param['values']:
                self.log.info('Fetching data for {0}={1}...'.format(
                    param_name,
                    value,
                ))
                self._metadata.update({param_name: value})
                post_data.update({param_name: value})
                self.upload_data(call_api(url=url, data=post_data))
        else:
            self.upload_data(call_api(url=url, data=post_data))
