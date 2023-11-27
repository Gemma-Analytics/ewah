from ewah.hooks.airtable import EWAHAirtableHook

from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC


class EWAHAirtableOperator(EWAHBaseOperator):
    _NAMES = ["airtable"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
    }

    _CONN_TYPE = EWAHAirtableHook.conn_type

    def __init__(self, base_id, table_id, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.table_id = table_id
        self.base_id = base_id

    def ewah_execute(self, context):
        self.upload_data(self.source_hook.get_table_data(self.base_id, self.table_id))
