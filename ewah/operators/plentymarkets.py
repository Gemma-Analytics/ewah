from ewah.constants import EWAHConstants as EC
from ewah.hooks.plentymarkets import EWAHPlentyMarketsHook
from ewah.operators.base import EWAHBaseOperator

from datetime import datetime, date


class EWAHPlentyMarketsOperator(EWAHBaseOperator):
    _NAMES = ["plentymarkets"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHPlentyMarketsHook.conn_type

    def __init__(
        self,
        resource=None,
        additional_api_call_params=None,
        batch_size=10000,
        request_method="get",
        post_request_payload=None,
        expand_field=None, #name of the field to expand
        *args,
        **kwargs,
    ):
        kwargs["primary_key"] = kwargs.get("primary_key", "id")
        resource = resource or kwargs.get("target_table_name")
        
        # expand_field has only been implemented for full refresh strategy
        assert (
            kwargs["extract_strategy"] == EC.ES_FULL_REFRESH or expand_field is None
        ), "expand_field can only be used with full refresh strategy"
        
        if kwargs["extract_strategy"] == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = kwargs.get("subsequent_field", "updatedAt")
            assert (
                EWAHPlentyMarketsHook.format_resource(resource)
                in EWAHPlentyMarketsHook._INCREMENTAL_FIELDS.keys()
            ), "{0} is not subsequent loadable!".format(resource)
        if kwargs["extract_strategy"] == EC.ES_INCREMENTAL:
            assert (
                EWAHPlentyMarketsHook.format_resource(resource)
                in EWAHPlentyMarketsHook._INCREMENTAL_FIELDS.keys()
            ), "{0} is not incrementally loadable!".format(resource)
        super().__init__(*args, **kwargs)

        assert isinstance(additional_api_call_params, (type(None), dict))
        self.resource = resource
        self.additional_api_call_params = additional_api_call_params
        self.batch_size = batch_size
        self.request_method = request_method
        self.post_request_payload = post_request_payload
        self.expand_field = expand_field

    # method to expand a field that contains an array of dictionaries
    def expand_field_data(self, batch):
        if not self.expand_field:
            return batch

        expanded_data = []

        while batch:
            row = batch.pop(0)

            # skip if field doesn't exist in this row and just append row as is
            if self.expand_field not in row:
                expanded_data.append(row)
                continue
            
            # get the field data
            field_data = row.pop(self.expand_field, [])

            # If field exists but is None/empty, default to empty list
            if not field_data:
                expanded_data.append(row)
                continue

            # Create a new row for each item in the field
            for item in field_data:
                new_row = row.copy()
                # Create new columns with prefixed names to avoid conflicts
                prefixed_item = {f"{self.expand_field}_{k}": v for k, v in item.items()}
                new_row.update(prefixed_item)
                expanded_data.append(new_row)

        return expanded_data

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
            # This is likely to be a text field - need a date instead
            if not isinstance(data_from, date):
                if isinstance(data_from, datetime):
                    data_from = data_from.date()
                elif isinstance(data_from, str):
                    data_from = datetime.fromisoformat(data_from).date()
                else:
                    raise Exception(
                        "Data type of {0} is invalid!".format(self.subsequent_field)
                    )
        else:
            data_from = self.data_from

        total_records = 0
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            data_from=data_from,
            data_until=self.data_until,
            additional_params=self.additional_api_call_params,
            batch_size=self.batch_size,
            request_method=self.request_method,
            post_request_payload=self.post_request_payload,
        ):
            batch_size = len(batch)
            self.log.info(f"Processing batch of {batch_size} records")

            if self.expand_field:
                self.log.info(f"Expanding fields: {self.expand_field}")
            batch = self.expand_field_data(batch)

            expanded_size = len(batch)
            if expanded_size != batch_size:
                self.log.info(f"Batch size after expansion: {expanded_size} records")

            self.upload_data(batch)
            total_records += expanded_size

        self.log.info(
            f"Completed extraction of {total_records} total records for {self.resource}"
        )
