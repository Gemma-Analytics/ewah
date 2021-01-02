from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.base import EWAHBaseHook as BaseHook

import googlemaps


class EWAHGMapsOperator(EWAHBaseOperator):
    """Gets address data from a custom SQL fired against the DWH.

    Params:
        - address_sql - a (templated) SQL statement that fetches the data
            as required; only one column called 'address' will be respected.
            Any additional columns will be ignored, and failure to provide
            such a column will cause failure of the operator.
    """

    _NAMES = ["gmaps", "google_maps", "googlemaps"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,  # use templating for incremental usecases
    }

    def __init__(self, address_sql, *args, **kwargs):
        self.template_fields.add("address_sql")
        kwargs["primary_key_column_name"] = "address"
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
                address = addresses.pop(0)["address"]
            except:
                raise Exception("Your SQL did not provide an address column!")

            data += [
                {
                    "address": address,
                    "geocode_result": client.geocode(address),
                }
            ]

        self.upload_data(data)
