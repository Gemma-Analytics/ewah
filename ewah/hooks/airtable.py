from ewah.hooks.base import EWAHBaseHook
from pyairtable import Table


class EWAHAirtableHook(EWAHBaseHook):
    _ATTR_RELABEL = {
        "api_key": "password",
    }

    conn_name_attr = "ewah_airtable_conn_id"
    default_conn_name = "ewah_airtable_default"
    conn_type = "ewah_airtable"
    hook_name = "EWAH Airtable Connection"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "host", "login"],
            "relabeling": {"password": "Airtable API Key"},
        }

    def get_table_data(self, base_id, table_id):
        data = Table(self.conn.api_key, base_id, table_id).all()
        for datum in data:
            # All fields are contained in a dictionary
            # -> Move it into the first level
            datum.update(datum.pop("fields"))
        return data
