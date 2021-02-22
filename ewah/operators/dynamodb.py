from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator

import boto3


class EWAHDynamoDBOperator(EWAHBaseOperator):

    _NAMES = ["dynamodb"]

    # For incremental loading, use Kinesis Firehose to push changes to S3
    # and use S3 operator instead
    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _REQUIRES_COLUMNS_DEFINITION = False

    def __init__(
        self,
        partition_key,
        sort_key=None,
        source_table_name=None,  # defaults to target_table_name
        pagination_limit=None,  # optionally set a pagination limit
        region_name=None,  # must provide region, alternatively via connection
        filter_expression=None,
        *args,
        **kwargs
    ):

        source_table_name = source_table_name or kwargs.get("target_table_name")
        kwargs["primary_key_column_name"] = [partition_key]
        if sort_key:
            kwargs["primary_key_column_name"].append(sort_key)
        super().__init__(*args, **kwargs)
        self.source_table_name = source_table_name
        self.pagination_limit = pagination_limit
        self.region_name = region_name
        self.filter_expression = filter_expression

    def ewah_execute(self, context):
        for batch in self.source_hook.get_dynamodb_data_in_batches(
            table_name=self.source_table_name,
            region=self.region_name,
            pagination_limit=self.pagination_limit,
            filter_expression=self.filter_expression,
        ):
            self.upload_data(batch)
