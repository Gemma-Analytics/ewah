from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.constants import EWAHConstants as EC

from airflow.hooks.base_hook import BaseHook

import googlemaps

class EWAHGMapsOperator(EWAHBaseOperator):
    """ Gets address data from a custom SQL fired against the DWH.

    Params:
        - address_sql - a (templated) SQL statement that fetches the data
            as required; only one column called 'address' will be respected.
            Any additional columns will be ignored, and failure to provide
            such a column will cause failure of the operator.
    """

    template_fields = ('address_sql',)

    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = True

    def __init__(self,
        address_sql,
    *args, **kwargs):

        kwargs['primary_key_column_name'] = 'address'
        super().__init__(*args, **kwargs)

        self.address_sql = address_sql

    def ewah_execute(self, context):
        dwh_hook = self.upload_hook

        addresses = dwh_hook.execute_and_return_result(
            sql=self.address_sql,
            return_dict=True,
        )

        api_key = self.source_conn.password

        client = googlemaps.Client(key=api_key)

        data = []
        while addresses:
            try:
                address = addresses.pop(0)['address']
            except:
                raise Exception('Your SQL did not provide an address column!')

            data += [{
                'address': address,
                'geocode_result': client.geocode(address),
                }]

        self.upload_data(data)
