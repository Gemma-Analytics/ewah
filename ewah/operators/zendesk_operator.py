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

    _base_url = \
        'https://{support_url}.zendesk.com/api/v2/incremental/{resource}.json'

    _accepted_resources = [
        'tickets',
        'ticket_metric_events',
        'users',
    ]
    def __init__(self,
        support_url,
        resource,
        auth_type,
        data_from,
    *args, **kwargs):

        if not resource in self._accepted_resources:
            raise Exception('resource "{0}" not supported!'.format(resource))

        if not auth_type in ['basic_auth']:
            raise Exception('auth_type must be basic_auth!')

        #if not type(page_limit) == int or page_limit < 1 or page_limit > 100:
        #    raise Exception('page_limit must be a positive interger <= 100!')

        if not type(data_from) == datetime:
            raise Exception('data_from must be a datetime object!')

        kwargs['primary_key_column_name'] = \
            kwargs.get('primary_key_column_name', 'id')

        super().__init__(*args, **kwargs)

        self.support_url = support_url
        self.resource = resource
        self.auth_type = auth_type
        self.data_from = data_from
        #self.page_limit = page_limit

    def execute(self, context):
        # Make sure it is at least 70 seconds after next_execution_date!
        td70 = timedelta(seconds=70)
        while datetime.now() < (context['next_execution_date'] + td70):
            self.log.info('delaying execution...')
            time.sleep(5)

        url = self._base_url.format(**{
            'support_url': self.support_url,
            'resource': self.resource,
        })

        if self.test_if_target_table_exists():
            data_from = context['execution_date']
        else:
            data_from = self.data_from
        data_from = data_from.timetuple()
        data_until = time.mktime(context['next_execution_date'].timetuple())

        params = {
            'start_time': str(time.mktime(data_from)),
        }

        conn = BaseHook.get_connection(self.source_conn_id)
        if self.auth_type == 'basic_auth':
            auth = HTTPBasicAuth(conn.login, conn.password)
        else:
            raise Exception('auth_type not implemented!')

        # Time-based incremental exports, run the first, the paginate!
        self.log.info('Calling Zendesk api.\nurl: {0}\nparams:{1}'.format(
            url,
            str(params),
        ))
        r = requests.get(url, params=params, auth=auth)
        data = json.loads(r.text)
        while r.status_code == 200 \
            and not data.get('end_of_stream') \
            and data.get('end_time') \
            and data.get('end_time') <= data_until:
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
