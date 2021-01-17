from ewah.hooks.base import EWAHBaseHook

import mailchimp3

from typing import List, Dict, Any


class EWAHMailchimpHook(EWAHBaseHook):
    # API docs: https://mailchimp.com/developer/api/marketing/

    _ATTR_RELABEL = {
        "api_key": "password",
        "user": "login",
    }

    conn_name_attr = "ewah_mailchimp_conn_id"
    default_conn_name = "ewah_mailchimp_default"
    conn_type = "ewah_mailchimp"
    hook_name = "EWAH Mailchimp Connection"

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        return {
            "hidden_fields": ["port", "schema", "extra", "host"],
            "relabeling": {
                "login": "User",
                "password": "API Key",
            },
        }

    def get_data_in_batches(
        self, resource: str, batch_size: int = 10000
    ) -> List[Dict[str, Any]]:
        client = mailchimp3.MailChimp(mc_user=self.conn.user, mc_api=self.conn.api_key)
        mc_resource = getattr(client, resource)
        keepgoing = True
        offset = 0
        while keepgoing:
            data = mc_resource.all(count=batch_size, offset=offset)[resource]
            if data:
                yield data
                offset += batch_size
            else:
                keepgoing = False
