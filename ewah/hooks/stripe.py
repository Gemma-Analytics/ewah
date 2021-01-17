from ewah.hooks.base import EWAHBaseHook

from typing import Dict, List, Any, Optional

import stripe


class EWAHStripeHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "api_key": "password",
    }

    conn_name_attr = "ewah_stripe_conn_id"
    default_conn_name = "ewah_stripe_default"
    conn_type = "ewah_stripe"
    hook_name = "EWAH Stripe Connection"

    _listable = stripe.api_resources.abstract.ListableAPIResource
    _singleton = stripe.api_resources.abstract.SingletonAPIResource

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        return {
            "hidden_fields": ["port", "schema", "extra", "host", "login"],
            "relabeling": {
                "password": "API Key",
            },
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # authenticate using API key
        stripe.api_key = self.conn.api_key

    def get_resource(self, resource: str):
        """Utility function to retrieve a particular resource.

        :param resource: path of the resource. E.g. "issuing.Transaction" to get
            stripe.api_resources.issuing.Transaction.
        :return: Stripe API Resource.
        """
        result = stripe.api_resources
        for subresource in resource.split("."):
            result = getattr(result, subresource)
        return result

    def get_data_in_batches(
        self,
        resource: str,
        expand: Optional[List[str]] = None,
        batch_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        # get data
        data = []
        resource = self.get_resource(resource=resource)
        if issubclass(resource, self._listable):
            all_items = resource.list(limit=100, expand=expand)
            for item in all_items.auto_paging_iter():
                data += [item.to_dict_recursive()]
                if len(data) >= batch_size:
                    yield data
                    data = []
            if data:  # Last batch
                yield data
        elif issubclass(resource, self._singleton):
            data = [resource.retrieve(expand=expand).to_dict_recursive()]
            # singletons have no id, but we need it as PK -> set it manually:
            data[0]["id"] = 1
            assert len(data) == 1, "Not a singleton!"
            yield data
        else:
            raise Exception("Invalid resource!")
