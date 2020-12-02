from ewah.operators.base_operator import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import requests
import json
import urllib
import time


class EWAHHubspotOperator(EWAHBaseOperator):

    _IS_INCREMENTAL = False
    _IS_FULL_REFRESH = True

    _REQUIRES_COLUMNS_DEFINITION = False

    _BASE_URL = 'https://api.hubapi.com/crm/v3/objects/{0}'
    _PROPERTIES_URL = 'https://api.hubapi.com/crm/v3/properties/{0}'

    _OBJECTS = [
        'companies',
        'contacts',
        'deals',
        'feedback_submissions',
        'line_items',
        'products',
        'tickets',
        'quotes',
    ]

    def __init__(self, *args, object=None, properties=None, **kwargs):
        if object is None:
            object = kwargs.get('target_table_name')
        assert object in self._OBJECTS, 'Object {0} is invalid!'.format(object)
        kwargs['primary_key_column_name'] = 'id'
        super().__init__(*args, **kwargs)
        self.object = object
        self.properties = properties

    def ewah_execute(self, context):
        params = {'hapikey': self.source_conn.password, 'limit': 100}
        headers = {'accept': 'application/json'}
        url = self._BASE_URL.format(self.object)
        url_properties = self._PROPERTIES_URL.format(self.object)

        # set properties list
        properties = self.properties
        if properties is None:
            # get properties via API call if none are given
            request = requests.get(
                url=url_properties,
                params=params,
                headers=headers,
            )
            assert request.status_code == 200, request.text
            result = json.loads(request.text)['results']
            properties = [property['name'] for property in result]
        params.update({'properties': properties})

        # get and upload data
        keepgoing = True
        counter = 0
        while keepgoing:
            time.sleep(0.2)
            counter += 1
            _msg = 'Loading data - request number {0}...'.format(str(counter))
            self.log.info(_msg)
            request = requests.get(url=url, params=params, headers=headers)
            if request.status_code == 414:
                _msg = 'ERROR! Too many properties '
                _msg += '- please use a smaller, custom set of properties!'
                _msg += '\n\navailable properties:\n\n\t'
                _msg += '\n\t'.join(properties)
                self.log.info(_msg)
            assert request.status_code == 200, request.text
            response = json.loads(request.text)
            # keep going as long as a link is shipped in the response
            keepgoing = response.get('paging', {}).get('next', {}).get('after')
            params.update({'after': keepgoing})
            data = response['results'] or []
            for datum in data:
                # Expand the properties field! Otherwise, the properties are
                # just a JSON in a single column called "properties"
                datum.update(datum.pop('properties', {}))
            self.upload_data(data)
