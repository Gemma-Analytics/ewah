from ewah.operators.base_operator import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from datetime import timedelta
import requests
import json
import time

class EWAHAircallOperator(EWAHBaseOperator):

    _ACCEPTED_LOAD_STRATEGIES = {
        EC.LS_FULL_REFRESH: True,
        EC.LS_INCREMENTAL: True,
        EC.LS_APPENDING: False,
    }

    _REQUIRES_COLUMNS_DEFINITION = False

    _BASE_URL = 'https://api.aircall.io/v1/{0}'

    _RESOURCES = {
        'users': {'incremental': True},
        'teams': {},
        'calls': {'incremental': True},
        'numbers': {'incremental': True},
        'contacts': {'incremental': True},
        'tags': {},
    }

    def __init__(self,
        resource=None,
        wait_between_pages=1,
        reload_data_chunking=None,
    *args, **kwargs):
        kwargs['primary_key_column_name'] = 'id'
        resource = resource or kwargs.get('target_table_name')
        super().__init__(*args, **kwargs)

        assert resource in self._RESOURCES, 'Invalid resource!'
        self.resource = resource
        assert isinstance(wait_between_pages, int) and wait_between_pages >= 0
        self.wait_between_pages = wait_between_pages
        assert isinstance(reload_data_chunking, (type(None), timedelta))
        self.reload_data_chunking = reload_data_chunking

    def _upload_aircall_data(self, url, params, auth):
        # implement pagination, return data as list of dicts
        i = 0
        data = []
        while url:
            i += 1
            time.sleep(self.wait_between_pages)
            self.log.info('Requesting page {0}...'.format(str(i)))
            request = requests.get(url, params=params, auth=auth)
            assert request.status_code == 200, request.text
            response = json.loads(request.text)
            url = response.get('meta', {}).get('next_page_link')
            data += response.get(self.resource, [])
            if len(data) >= 10000:
                raise Exception('Error! 10k limit reached! Introduce chunking!')
        return self.upload_data(data)

    def ewah_execute(self, context):
        auth = requests.auth.HTTPBasicAuth(
            self.source_conn.login,
            self.source_conn.password,
        )
        url = self._BASE_URL.format(self.resource)
        params = {
            'per_page': 50, # maximum page size is 50
        }
        if self.load_strategy == EC.LS_INCREMENTAL \
            and self._RESOURCES[self.resource].get('incremental'):
            # incremental load
            data_from = self.load_data_from
            data_until = self.load_data_until

            # The API may only return max 10k records, thus enable usage of
            # chunking to reduce chunk-size to <10k records per time interval
            chunking = self.reload_data_chunking or (data_until - data_from)
            while data_from < data_until:
                params.update({
                    'from': int(time.mktime(data_from.timetuple())),
                    'to': int(time.mktime((data_from + chunking).timetuple())),
                })
                self._upload_aircall_data(url=url, params=params, auth=auth)
                data_from += chunking
        else:
            self._upload_aircall_data(url=url, params=params, auth=auth)
