from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator


class EWAHMongoDBOperator(EWAHBaseOperator):

    _NAMES = ["mongo", "mongodb"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _REQUIRES_COLUMNS_DEFINITION = False

    def __init__(
        self,
        source_collection_name=None,  # defaults to target_table_name
        source_database_name=None,  # Not required if specified in connection
        timestamp_field=None,  # required for use with data_from and data_until
        single_column_mode=False,  # If True, throw all data as json into one col
        batch_size=100000,
        *args,
        **kwargs
    ):

        self.source_collection_name = (
            source_collection_name or kwargs["target_table_name"]
        )
        self.source_database_name = source_database_name

        if single_column_mode:
            if kwargs.get("columns_definition"):
                raise Exception(
                    "single_column_mode is not compatible with " + "columns_definition!"
                )
            if not kwargs.get("extract_strategy") == EC.ES_FULL_REFRESH:
                raise Exception(
                    "single_column_mode is only compatible with "
                    + "extract_strategy = {0}!".format(EC.ES_FULL_REFRESH)
                )
            if kwargs.get("update_on_columns"):
                raise Exception(
                    "single_column_mode is not compatible with " + "update_on_columns!"
                )
            if kwargs.get("primary_key_column_name"):
                raise Exception(
                    "single_column_mode is not compatible with "
                    + "primary_key_column_name!"
                )
        self.single_column_mode = single_column_mode

        if kwargs.get("extract_strategy") == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = kwargs.get(
                "subsequent_field",
                timestamp_field or kwargs.get("primary_key_column_name", None),
            )

        super().__init__(*args, **kwargs)

        if (
            self.load_data_from
            or self.load_data_until
            or self.reload_data_from
            or (self.extract_strategy == EC.ES_INCREMENTAL)
        ):
            if not timestamp_field:
                err_msg = "If using reload_data_from, load_data_from, or load_"
                err_msg += "data_until, you must also specify timestamp_field!"
                raise Exception(err_msg)
        self.timestamp_field = timestamp_field
        self.batch_size = batch_size

    def upload_data(self, data):
        if self.single_column_mode:
            # load all data into a single column called 'document'
            data = [{"document": x} for x in data]
            super().upload_data(data)
        else:
            # normal data loading mode
            super().upload_data(data)

    def ewah_execute(self, context):
        base_filters = []
        # data_from and data_until filter expression
        if self.timestamp_field:
            if self.data_from:
                base_filters += [{self.timestamp_field: {"$gte": self.data_from}}]
            if self.data_until:
                base_filters += [{self.timestamp_field: {"$lt": self.data_until}}]
        if self.subsequent_field and self.test_if_target_table_exists():
            base_filters += [
                {
                    self.subsequent_field: {
                        "$gte": self.get_max_value_of_column(self.subsequent_field)
                    }
                }
            ]
        if base_filters:
            filter_expressions = {"$and": base_filters}
        else:
            filter_expressions = None

        for batch in self.source_hook.get_data_in_batches(
            collection=self.source_collection_name,
            database=self.source_database_name,
            filter_expression=filter_expressions,
            batch_size=self.batch_size,
        ):
            self.upload_data(batch)
