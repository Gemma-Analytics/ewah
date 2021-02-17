from ewah.operators.base import EWAHBaseOperator
from ewah.ewah_utils.python_utils import is_iterable_not_string
from ewah.constants import EWAHConstants as EC

from google.ads.google_ads.client import GoogleAdsClient
from ewah.hooks.base import EWAHBaseHook as BaseHook

from datetime import datetime, timedelta
from copy import deepcopy


class EWAHGoogleAdsOperator(EWAHBaseOperator):

    _NAMES = ["gads", "google_ads"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: True,
    }

    _REQUIRED_KEYS = (
        # You must have a developer_token to use the Google Ads API!
        "developer_token",
        "client_id",
        "client_secret",
        "refresh_token",
    )

    def __init__(
        self,
        fields,  # dict of fields, e.g. {'campaign': ['id', 'name']}
        metrics,  # list of metrics to retrieve -
        resource,  # name of the resource, e.g. ad_group
        client_id,
        conditions=None,  # list of conditions, e.g. ["ad_group.status = 'ENABLED'"]
        *args,
        **kwargs
    ):
        """
        recommendation: leave data_until=None and use a timedelta for
        data_from.
        """

        if kwargs.get("columns_definition"):
            raise Exception(
                "columns_definition is not accepted for this " + "operator!"
            )

        kwargs["update_on_columns"] = [
            col.replace(".", "_")
            for col in self.get_select_statement(fields)
            if col[:7] == "segment"
        ] + ["{0}_resource_name".format(resource)]

        if conditions and not is_iterable_not_string(conditions):
            raise Exception('Argument "conditions" must be a list!')

        if not (fields.get("segments") and ("date" in fields["segments"])):
            raise Exception("fields list MUST contain segments.date!")

        # must use deepcopy due to airflow calling field by reference
        # and the update in the following row adding the metrics to the field,
        # which in turn screws with the update_on_columns definition above
        self.fields_dict = deepcopy(fields)
        self.fields_dict.update({"metrics": deepcopy(metrics)})
        self.fields_list = self.get_select_statement(self.fields_dict)
        self.resource = resource
        self.client_id = str(client_id)
        self.conditions = conditions

        super().__init__(*args, **kwargs)

    def get_select_statement(self, dict_format, prefix=None):
        # create the list of fields for the SELECT statement
        if prefix is None:
            prefix = ""
        elif not prefix[-1] == ".":
            prefix += "."
        fields = []
        for key, value in dict_format.items():
            for item in value:
                if type(item) == dict:
                    fields += self.get_select_statement(item, prefix + key)
                else:
                    fields += [prefix + key + "." + item]
        return fields

    def ewah_execute(self, context):
        # Task execution happens here
        def get_data_from_ads_output(fields_dict, values, prefix=None):
            if prefix is None:
                prefix = ""
            elif not prefix[-1] == "_":
                prefix += "_"
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
                        data.update(
                            get_data_from_ads_output(
                                fields_dict=item,
                                values=node,
                                prefix=prefix
                                + key,  # e.g. 2a '' + 'ad_group_criterion'
                            )
                        )
                    else:
                        # e.g. 1: {'' + 'metrics' + '_' + 'clicks': row.metrics.clicks.value}
                        # e.g. 2b: {'ad_group_criterion_' + 'keyeword' + '_' + 'text': row.ad_group_criterion.keyword.text.value}
                        if hasattr(getattr(node, item), "value"):
                            data.update(
                                {prefix + key + "_" + item: getattr(node, item).value}
                            )
                        else:
                            # some node ends don't respond to .value but are
                            #   already the value
                            data.update(
                                {prefix + key + "_" + item: getattr(node, item)}
                            )
            return data

        conn = self.source_conn.extra_dejson
        credentials = {}
        for key in self._REQUIRED_KEYS:
            if not key in conn.keys():
                raise Exception("{0} must be in connection extra json!".format(key))
            credentials[key] = conn[key]

        # build the query
        query = "SELECT {0} FROM {1} WHERE segments.date {2} {3}".format(
            ", ".join(self.fields_list),
            self.resource,
            "BETWEEN '{0}' AND '{1}'".format(
                self.data_from.strftime("%Y-%m-%d"),
                self.data_until.strftime("%Y-%m-%d"),
            ),
            ("AND" + " AND ".join(self.conditions)) if self.conditions else "",
        )

        self.log.info("executing this google ads query:\n{0}".format(query))
        cli = GoogleAdsClient.load_from_dict(credentials)
        service = cli.get_service("GoogleAdsService")
        search = service.search(
            self.client_id.replace("-", ""),
            query=query,
        )
        data = [row for row in search]

        # get into uploadable format
        upload_data = []
        fields_dict = deepcopy(self.fields_dict)
        fields_dict.update({self.resource: ["resource_name"]})
        while data:
            datum = data.pop(0)
            upload_data += [
                get_data_from_ads_output(
                    fields_dict,
                    datum,
                )
            ]

        self.upload_data(upload_data)
