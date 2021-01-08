from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.hubspot import EWAHHubspotHook


class EWAHHubspotOperator(EWAHBaseOperator):

    _NAMES = ["hubspot"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _REQUIRES_COLUMNS_DEFINITION = False

    _CONN_TYPE = EWAHHubspotHook.conn_type

    def __init__(
        self,
        *args,
        object=None,
        properties=None,
        exclude_properties=None,
        associations=None,
        **kwargs
    ):
        if object is None:
            object = kwargs.get("target_table_name")
        assert (
            object in EWAHHubspotHook.ACCEPTED_OBJECTS
        ), "Object {0} is invalid!".format(object)
        kwargs["primary_key_column_name"] = "id"
        super().__init__(*args, **kwargs)
        self.object = object
        if isinstance(properties, str):
            properties = [properties]
        if isinstance(exclude_properties, str):
            exclude_properties = [exclude_properties]
        if isinstance(associations, str):
            associations = [associations]
        nonetype = type(None)
        assert isinstance(properties, (list, nonetype))
        assert isinstance(exclude_properties, (list, nonetype))
        assert isinstance(associations, (list, nonetype))
        self.properties = properties
        self.exclude_properties = exclude_properties
        self.associations = associations

    def ewah_execute(self, context):
        for batch in self.source_hook.get_data_in_batches(
            object=self.object,
            properties=self.properties,
            exclude_properties=self.exclude_properties,
            associations=self.associations,
        ):
            self.upload_data(batch)
