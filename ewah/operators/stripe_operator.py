from ewah.operators.base_operator import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import stripe

class EWAHStripeOperator(EWAHBaseOperator):

    _IS_INCREMENTAL = False
    _IS_FULL_REFRESH = True

    _REQUIRES_COLUMNS_DEFINITION = False

    _UPLOAD_AFTER_ROWS = 20000

    _listable = stripe.api_resources.abstract.ListableAPIResource
    _singleton = stripe.api_resources.abstract.SingletonAPIResource

    def __init__(self, *args, resource=None, expand=None, **kwargs):
        if resource is None:
            resource = kwargs.get('target_table_name')
        _msg = 'Not a valid resource: {0}!'.format(resource)
        assert hasattr(stripe, resource), _msg
        resource = getattr(stripe, resource)
        # use issubclass instead of isinstance
        assert issubclass(resource, (self._listable, self._singleton)), _msg
        kwargs['primary_key_column_name'] = 'id'
        super().__init__(*args, **kwargs)
        self.resource = resource
        self.expand = expand


    def ewah_execute(self, context):
        # authenticate using API key
        stripe.api_key = self.source_conn.password

        # get data
        data = []
        resource = self.resource
        if issubclass(resource, self._listable):
            all_items = resource.list(limit=100, expand=self.expand)
            for item in all_items.auto_paging_iter():
                data += [item.to_dict_recursive()]
                if len(data) > self._UPLOAD_AFTER_ROWS:
                    # intermittend data upload
                    self.upload_data(data)
                    data = []
        elif issubclass(resource, self._singleton):
            data = [resource.retrieve(expand=self.expand).to_dict_recursive()]
            # singletons have no id, but we need it as PK -> set it manually:
            data[0]['id'] = 1
            assert len(data) == 1, 'Not a singleton!'
        else:
            raise Exception('Invalid resource!')

        # final data upload
        self.upload_data(data)
