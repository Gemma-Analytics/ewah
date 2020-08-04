from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta

from requests.auth import HTTPBasicAuth
import requests
import json
import time

class EWAHShopifyOperator(EWAHBaseOperator):

    # template_fields = ()

    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = False

    _acceptable_api_versions = [
        '2020-07',
    ]
    _current_api_version = '2020-07'
    _base_url = 'https://{shop}.myshopify.com/admin/api/{version}/{object}.json'

    # Set all accepted objects with their potential filter fields with defaults
    #   Note: always expected fields updated_at_max, updated_at_min, limit, id
    #   the key _is_drop_and_replace indicates objects that are only able
    #   to load with a full refresh
    _default_timestamp_fields = ('updated_at_min', 'updated_at_max')
    _accepted_objects = {
        'customers': {
            'ids': None,
            'since_id': None,
        },
        'discount_codes': {
            '_is_drop_and_replace': True,
        },
        'events': {
            '_timestamp_fields': ('created_at_min', 'created_at_max'),
            'since_id': None,
            'filter': None,
            'verb': None,
        },
        'orders': {
            'ids': None,
            'since_id': None,
            'status': 'any',
            'financial_status': None,
            'fulfillment_status': None,
            'fields': None,
        },
        'price_rules': {
            'since_id': None,
        },
        'products': {
            'ids': None,
            'since_id': None,
            'title': None,
            'vendor': None,
            'handle': None,
            'product_type': None,
            'collection_id': None,
            'published_status': None,
        },
    }

    def __init__(self,
        shop_id,
        shopify_object,
        auth_type,
        filter_fields={},
        api_version=None,
        page_limit=250,
    *args, **kwargs):

        if not shopify_object in self._accepted_objects.keys():
            raise Exception('{0} is not in the list of accepted objects!' + \
                ' accepted objects: {1}'.format(
                    shopify_object,
                    ', '.join(self._accepted_objects.keys())
                ))

        if self._accepted_objects[shopify_object].get('_is_drop_and_replace'):
            kwargs['drop_and_replace'] = True

        if not auth_type in ['access_token', 'basic_auth']:
            raise Exception('auth_type must be access_token or basic_auth!')

        if not type(filter_fields) == dict:
            raise Exception('filter_fields must be a dictionary!')
        else:
            for key, value in filter_fields.items():
                if not key in self._accepted_objects['shopify_object'].keys():
                    raise Exception('invalid key {0} in filter fields!'.format(
                        key,
                    ))

        api_version = api_version or self._current_api_version
        if not api_version in self._acceptable_api_versions:
            raise Exception(
                '{0} is not a valid api version! valid versions: {1}'.format(
                    api_version,
                    ', '.join(self._acceptable_api_versions),
                )
            )

        if not type(page_limit) == int or page_limit > 250 or page_limit < 1:
            raise Exception( \
                'Page limit must be a positive integer not exceeding 250!')

        kwargs['primary_key_column_name'] = \
            kwargs.get('primary_key_column_name', 'id')

        super().__init__(*args, **kwargs)

        self.shop_id = shop_id
        self.shopify_object = shopify_object
        self.auth_type = auth_type
        self.filter_fields = filter_fields
        self.api_version = api_version
        self.page_limit = page_limit

    def execute(self, context):
        # Get data from shopify via REST API

        url = self._base_url.format(**{
            'shop': self.shop_id,
            'version': self.api_version,
            'object': self.shopify_object,
        })

        object_metadata = self._accepted_objects[self.shopify_object]

        params = {
            key: val
            for key, val in object_metadata.items()
            if not val is None and not key[:1] == '_'
        }
        params.update(self.filter_fields)
        params.update({'limit': self.page_limit})
        if not self.drop_and_replace:
            timestamp_fields = object_metadata.get(
                '_timestamp_fields',
                self._default_timestamp_fields,
            )
            params.update({
                # Pendulum by coincidence converts to the correct string format
                timestamp_fields[1]: str(context['next_execution_date']),
            })
            if self.test_if_target_table_exists():
                params.update({
                    timestamp_fields[0]: str(context['execution_date']),
                })

        conn = BaseHook.get_connection(self.source_conn_id)
        if self.auth_type == 'access_token':
            headers = {
                'X-Shopify-Access-Token': conn.password,
            }
            kwargs_init = {
                'headers': headers,
                'params': params,
            }
            kwargs_links = {'headers': headers}
        elif self.auth_type == 'basic_auth':
            kwargs_init = {
                    'params': params,
                'auth': HTTPBasicAuth(conn.login, conn.password),
            }
            kwargs_links = {'auth': HTTPBasicAuth(conn.login, conn.password)}
        else:
            raise Exception('Authentication type not accepted!')

        # get and upload data
        self.log.info('Requesting data from REST API - url: {0}, params: {1}' \
            .format(url, str(params)))
        r = requests.get(url, **kwargs_init)
        while r.status_code == 200 \
            and r.headers.get('Link') \
            and r.headers['Link'][-10:] == 'rel="next"':
            self.upload_data(json.loads(r.text)[self.shopify_object])
            self.log.info('Requesting next page of data...')
            r = requests.get(r.headers.get('Link')[1:-13], **kwargs_links)

        if not r.status_code == 200:
            raise Exception('Shopify request returned an error {1}: {0}'.format(
                r.text,
                str(r.status_code),
            ))

        # The last page is not uploaded within the while loop!
        self.upload_data(json.loads(r.text)[self.shopify_object])
