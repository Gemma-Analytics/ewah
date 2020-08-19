from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.ewah_utils.python_utils import is_iterable_not_string
from ewah.constants import EWAHConstants as EC

from google.ads.google_ads.client import GoogleAdsClient
from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta
from copy import deepcopy

class EWAHGoogleAdsOperator(EWAHBaseOperator):

    template_fields = ('data_from', 'data_until')

    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = False

    _REQUIRED_KEYS = (
        'developer_token',
        'client_id',
        'client_secret',
        'refresh_token',
    )

    def __init__(self,
        fields, # dict of fields, e.g. {'campaign': ['id', 'name']}
        metrics, # list of metrics to retrieve -
        resource, # name of the resource, e.g. ad_group
        client_id,
        conditions=None, # list of conditions, e.g. ["ad_group.status = 'ENABLED'"]
        data_from=None, # can be datetime or timedelta relative to data_until
        data_until=None,
    *args, **kwargs):
        """
            recommendation: leave data_until=None and use a timedelta for
            data_from.
        """

        kwargs['update_on_columns'] = [
            col.replace('.', '_') for col in self.get_select_statement(fields)
        ]
        fields.update({'metrics': metrics})

        super().__init__(*args, **kwargs)

        if conditions and not is_iterable_not_string(conditions):
            raise Exception('Argument "conditions" must be a list!')

        if not (fields.get('segments') and ('date' in fields['segments'])):
            raise Exception('fields list MUST contain segments.date!')

        self.fields_list = self.get_select_statement(fields)
        self.fields_dict = fields
        self.resource = resource
        self.client_id = str(client_id)
        self.conditions = conditions
        self.data_from = data_from
        self.data_until = data_until

    def get_select_statement(self, dict_format, prefix=None):
        # create the list of fields for the SELECT statement
        if prefix is None:
            prefix = ''
        elif not prefix[-1] == '.':
            prefix += '.'
        fields = []
        for key, value in dict_format.items():
            for item in value:
                if type(item) == dict:
                    fields += self.get_select_statement(item, prefix + key)
                else:
                    fields += [prefix + key + '.' + item]
        return fields

    def execute(self, context):
        # Task execution happens here
        def get_data_from_ads_output(fields_dict, values, prefix=None):
            if prefix is None:
                prefix = ''
            elif not prefix[-1] == '_':
                prefix += '_'
                # e.g. 2b prefix = 'ad_group_criterion_'
            data = {}
            for key, value in fields_dict.items():
                # e.g. 1 key = 'metrics', value = ['impressions', 'clicks']
                # e.g. 2a key = 'ad_group_criterion', value = [{'keyword': ['text', 'match_type']}]
                # e.g. 2b key = 'keyword', value = ['text', 'match_type']
                node = getattr(values, key)
                # e.g. 1 node = row.metrics
                # e.g. 2a node = row.ad_group_criterion
                # e.g. 2b node = row.ad_group_criterion.keyword
                for item in value:
                    # e.g. 1 item = 'clicks'
                    # e.g. 2a item = {'keyword': ['text', 'match_type']}
                    # e.g. 2b item = 'text'
                    if type(item) == dict:
                        data.update(get_data_from_ads_output(
                            fields_dict=item,
                            values=node,
                            prefix=prefix + key, # e.g. 2a '' + 'ad_group_criterion'
                        ))
                    else:
                        # e.g. 1: {'' + 'metrics' + '_' + 'clicks': row.metrics.clicks.value}
                        # e.g. 2b: {'ad_group_criterion_' + 'keyeword' + '_' + 'text': row.ad_group_criterion.keyword.text.value}
                        if hasattr(getattr(node, item), 'value'):
                            data.update({
                                prefix + key + '_' + item: \
                                    getattr(node, item).value
                            })
                        else:
                            # some node ends don't respond to .value but are
                            #   already the value
                            data.update({
                                prefix + key + '_' + item: getattr(node, item)
                            })
            return data


        self.data_until = airflow_datetime_adjustments(self.data_until)
        self.data_until = self.data_until or context['next_execution_date']
        if isinstance(self.data_from, timedelta):
            self.data_from = self.data_until - self.data_from
        else:
            self.data_from = airflow_datetime_adjustments(self.data_from)
            self.data_from = self.data_from or context['execution_date']

        conn = BaseHook.get_connection(self.source_conn_id).extra_dejson
        credentials = {}
        for key in self._REQUIRED_KEYS:
            if not key in conn.keys():
                raise Exception('{0} must be in connection extra json!'.format(
                    key
                ))
            credentials[key] = conn[key]

        # build the query
        query = 'SELECT {0} FROM {1} WHERE segments.date {2} {3}'.format(
            ', '.join(self.fields_list),
            self.resource,
            "BETWEEN '{0}' AND '{1}'".format(
                self.data_from.strftime('%Y-%m-%d'),
                self.data_until.strftime('%Y-%m-%d'),
            ),
            ('AND' + ' AND '.join(self.conditions)) if self.conditions else '',
        )

        self.log.info('executing this google ads query:\n{0}'.format(query))
        cli = GoogleAdsClient.load_from_dict(credentials)
        service = cli.get_service("GoogleAdsService", version="v3")
        search = service.search(
            self.client_id.replace('-', ''),
            query=query,
        )
        data = [row for row in search]

        # get into uploadable format
        upload_data = []
        while data:
            datum = data.pop(0)
            upload_data += [get_data_from_ads_output(
                deepcopy(self.fields_dict),
                datum,
            )]

        self.upload_data(upload_data)
