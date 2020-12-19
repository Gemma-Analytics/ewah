from ewah.constants import EWAHConstants as EC
from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments

import boto3

class EWAHDynamoDBOperator(EWAHBaseOperator):

    # template_fields =

    # For incremental loading, use Kinesis Firehose to push changes to S3
    # and use S3 operator instead
    _IS_INCREMENTAL = False
    _IS_FULL_REFRESH = True

    _REQUIRES_COLUMNS_DEFINITION = False

    def __init__(self,
        source_table_name=None, # defaults to target_table_name
        pagination_limit=None, # optionally set a pagination limit
        region_name=None, # must provide region, alternatively via connection
        filter_expression=None,
    *args, **kwargs):

        source_table_name = source_table_name or kwargs.get('target_table_name')
        _msg = 'DynamoDBOperator requires primary_key_column_name!'
        assert kwargs.get('primary_key_column_name'), _msg
        super().__init__(*args, **kwargs)
        self.source_table_name = source_table_name
        self.pagination_limit = pagination_limit
        self.region_name = region_name
        self.filter_expression = filter_expression

    def ewah_execute(self, context):
        # Get credentials and connect to table
        conn = self.source_conn
        resource_kwargs = {
            'aws_access_key_id': conn.login,
            'aws_secret_access_key': conn.password,
        }
        if isinstance(conn.extra_dejson, dict):
            resource_kwargs.update(conn.extra_dejson)
        if self.region_name:
            resource_kwargs.update({'region_name': self.region_name})
        resource = boto3.resource('dynamodb', **resource_kwargs)
        table = resource.Table(self.source_table_name)

        # Paginate through table
        scan_kwargs = {}
        if self.pagination_limit:
            scan_kwargs.update({'Limit': self.pagination_limit})
        if self.filter_expression:
            scan_kwargs.update({'FilterExpression': self.filter_expression})
        keep_going = True
        while keep_going:
            response = table.scan(**scan_kwargs)
            self.upload_data(response.get('Items'))
            keep_going = response.get('LastEvaluatedKey') or False
            scan_kwargs.update({'ExclusiveStartKey': keep_going})
