from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta

from requests.auth import HTTPBasicAuth
import requests
import json
import time

class EWAHZendeskOperator(EWAHBaseOperator):

    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = False

    _base_url = 'https://{support_url}.zendesk.com/{endpoint}'
    # _base_url = \
        # 'https://{support_url}.zendesk.com/api/v2/incremental/{resource}.json'

    def __init__(self,
        support_url,
        resource,
        auth_type,
        data_from=None,
    *args, **kwargs):

        self._accepted_resources = {
            'tickets': {
                'function': self.execute_incremental_cursor_based,
                'datetime_field': 'updated_at',
            },
            'ticket_audits': {
                'function': self.execute_incremental_cursor_based,
                'datetime_field': 'created_at',
            },
            'ticket_metric_events': {
                'function': self.execute_incremental_time_based,
            },
            'users': {
                'function': self.execute_incremental_time_based,
            },
            'ticket_fields': {
                'function': self.get_custom_ticket_fields,
                'drop_and_replace': True,
            },
        }

        if not resource in self._accepted_resources.keys():
            raise Exception('resource "{0}" not supported!'.format(resource))

        if not auth_type in ['basic_auth']:
            raise Exception('auth_type must be basic_auth!')

        #if not type(page_limit) == int or page_limit < 1 or page_limit > 100:
        #    raise Exception('page_limit must be a positive interger <= 100!')

        if not type(data_from) == datetime:
            raise Exception('data_from must be a datetime object!')

        kwargs['primary_key_column_name'] = \
            kwargs.get('primary_key_column_name', 'id')

        if self._accepted_resources[resource].get('drop_and_replace'):
            kwargs['drop_and_replace'] = True

        super().__init__(*args, **kwargs)

        self.support_url = support_url
        self.resource = resource
        self.auth_type = auth_type
        self.data_from = data_from
        #self.page_limit = page_limit

    def execute(self, context):
        # Make sure it is at least 70 seconds after next_execution_date!
        # Reason: Immediate execution may miss the latest tickets
        td70 = timedelta(seconds=70)
        while datetime.now() < (context['next_execution_date'] + td70):
            self.log.info('delaying execution...')
            time.sleep(5)

        self.data_from = self.make_unix_datetime(self.get_data_from(context))
        self.data_until= self.make_unix_datetime(context['next_execution_date'])

        conn = BaseHook.get_connection(self.source_conn_id)
        if self.auth_type == 'basic_auth':
            self.auth = HTTPBasicAuth(conn.login, conn.password)
        else:
            raise Exception('auth_type not implemented!')

        self._metadata.update({'support_url': self.support_url})

        # run correct execute function
        return self._accepted_resources[self.resource]['function'](context)

    def get_data_from(self, context):
        if self.test_if_target_table_exists():
            return context['execution_date']
        else:
            return self.data_from or context['execution_date']

    def make_unix_datetime(self, dt):
        return str(int(time.mktime(dt.timetuple())))

    def get_custom_ticket_fields(self, context):
        url = self._base_url.format(
            support_url=self.support_url,
            endpoint='api/v2/ticket_fields.json',
        )
        req = requests.get(url, auth=self.auth)
        if not req.status_code == 200:
            raise Exception('Error {1} when calling Zendesk API: {0}'.format(
                req.text,
                str(req.status_code),
            ))
        data = json.loads(req.text)['ticket_fields']
        self.upload_data(data)

    def execute_incremental_cursor_based(self, context):
        if not self.resource in ['tickets', 'ticket_audits']:
            raise Exception('cursor-based load not implemented!')

        if self.resource == 'tickets':
            endpoint = 'api/v2/incremental/tickets/cursor.json'
            params = {
                'start_time': self.data_from,
                'include': 'metric_sets',
            }
            response_resource = 'tickets'
        elif self.resource == 'ticket_audits':
            endpoint = 'api/v2/ticket_audits.json'
            params = {
                'start_time': self.data_from,
            }
            response_resource = 'audits'

        datetime_field = self._accepted_resources[self.resource]
        datetime_field = datetime_field['datetime_field']

        url = self._base_url.format(
            support_url=self.support_url,
            endpoint=endpoint,
        )

        first_call = True
        while first_call or req.status_code == 200:
            if first_call:
                first_call = False
            self.log.info('Requesting a page of data...')
            req = requests.get(url, params=params, auth=self.auth)
            response = json.loads(req.text)
            if req.status_code == 200:
                data = response[response_resource]
                if data:
                    last_updated_at = datetime.strptime(
                        data[-1][datetime_field],
                        '%Y-%m-%dT%H:%M:%SZ',
                    )
                    self.upload_data(data)
                if response.get('end_of_stream') or \
                    last_updated_at > context['next_execution_date']:
                    break
                params = {'cursor': response.get('after_cursor')}

        if not req.status_code == 200:
            raise Exception('Error {1} when calling Zendesk API: {0}'.format(
                req.text,
                str(req.status_code),
            ))


    def execute_incremental_time_based(self, context):

        url = self._base_url.format(**{
            'support_url': self.support_url,
            'endpoint': 'api/v2/incremental/{0}.json'.format(self.resource)
        })

        params = {
            'start_time': self.data_from,
        }

        # Time-based incremental exports, run the first, the paginate!
        self.log.info('Calling Zendesk api.\nurl: {0}\nparams:{1}'.format(
            url,
            str(params),
        ))
        r = requests.get(url, params=params, auth=self.auth)
        data = json.loads(r.text)
        while r.status_code == 200 \
            and not data.get('end_of_stream') \
            and data.get('end_time') \
            and data.get('end_time') <= int(self.data_until):
            self.upload_data(data[self.resource]) # uploads previous request
            self.log.info('Requesting next page of data...')
            r = requests.get(data['next_page'], auth=auth) # new request
            data = json.loads(r.text)

        if not r.status_code == 200:
            raise Exception('Error {1} when calling Zendesk API: {0}'.format(
                r.text,
                str(r.status_code),
            ))

        # upload final request
        self.upload_data(data[self.resource])
