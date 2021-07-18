from ewah.hooks.base import EWAHBaseHook
import recurly


class EWAHRecurlyHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "api_token": "password",
    }

    conn_name_attr = "ewah_recurly_conn_id"
    default_conn_name = "ewah_recurly_default"
    conn_type = "ewah_recurly"
    hook_name = "EWAH Recurly Connection"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "host", "login"],
            "relabeling": {"password": "Baisc Auth API Key"},
        }

    @property
    def client(self):
        if not hasattr(self, "_client"):
            self._client = recurly.Client(self.conn.api_token)

        return self._client

    @staticmethod
    def validate_resource(resource):
        return hasattr(recurly.Client, "list_{0}".format(resource))

    @classmethod
    def recurly_to_dict(cls, recurly_object):
        final_dict = {}
        for key, value in vars(recurly_object).items():
            if isinstance(value, recurly.Resource):
                value = cls.recurly_to_dict(value)
            if isinstance(value, list):
                value = [
                    cls.recurly_to_dict(item)
                    if isinstance(item, recurly.Resource)
                    else item
                    for item in value
                ]
            final_dict[key] = value
        return final_dict

    def get_data_in_batches(
        self,
        resource,
        data_from=None,
        data_until=None,
        batch_size=10000,
    ):
        assert isinstance(batch_size, int) and batch_size > 0
        params = {
            "limit": 200,  # maximum limit per call (overwrite default of 20)
            "sort": "updated_at",
            "order": "asc",  # see docs - don't use desc with updated_at sort!
        }
        if data_from:
            params["begin_time"] = data_from.isoformat()
        if data_until:
            params["end_time"] = data_until.isoformat()

        # Serve data in batches
        data = []
        i = 0
        for item in getattr(self.client, "list_{0}".format(resource))(
            params=params
        ).items():
            i += 1
            data.append(self.recurly_to_dict(item))
            if i == batch_size:
                yield data
                data = []
                i = 0

        if data:  # last batch
            yield data
