from ewah.hooks.base import EWAHBaseHook

from typing import Optional, List, Dict, Any
from collections import defaultdict

import boto3


class EWAHAWSHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "access_key_id": "login",
        "secret_access_key": "password",
        "region": "schema",
        "role_arn": "host",
    }

    conn_name_attr = "ewah_aws_conn_id"
    default_conn_name = "ewah_aws_default"
    conn_type = "ewah_aws"
    hook_name = "EWAH AWS Connection"

    _resources = defaultdict(dict)

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["extra", "port"],
            "relabeling": {
                "login": "AWS Access Key ID",
                "password": "AWS Secret Access Key",
                "schema": "Region",
                "host": "[Optional] Role ARN",
            },
        }

    def get_boto_resource(self, resource: str, region: Optional[str] = None):
        aws_region = region or self.conn.region
        if not self._resources[aws_region].get(resource):
            if self.conn.role_arn:
                # Must use STS service to assume role before accessing service
                sts_client = boto3.client(
                    "sts",
                    aws_access_key_id=self.conn.access_key_id,
                    aws_secret_access_key=self.conn.secret_access_key,
                    region_name=aws_region,
                )
                temp_credentials = sts_client.assume_role(
                    RoleArn=self.conn.role_arn,
                    RoleSessionName="EWAH",
                )["Credentials"]
                self._resources[aws_region][resource] = boto3.resource(
                    resource,
                    aws_access_key_id=temp_credentials["AccessKeyId"],
                    aws_secret_access_key=temp_credentials["SecretAccessKey"],
                    aws_session_token=temp_credentials["SessionToken"],
                    region_name=aws_region,
                )
            else:
                self._resources[aws_region][resource] = boto3.resource(
                    resource,
                    aws_access_key_id=self.conn.access_key_id,
                    aws_secret_access_key=self.conn.secret_access_key,
                    region_name=aws_region,
                )
        return self._resources[aws_region][resource]

    def get_dynamodb_data_in_batches(
        self,
        table_name: str,
        region: Optional[str] = None,
        batch_size: int = 10000,
        pagination_limit: Optional[int] = None,
        filter_expression=None,
    ) -> List[Dict[str, Any]]:
        resource = self.get_boto_resource(resource="dynamodb", region=region)
        table = resource.Table(table_name)

        # build scan kwargs
        scan_kwargs = {}
        if pagination_limit:
            scan_kwargs["Limit"] = pagination_limit
        if filter_expression:
            scan_kwargs["FilterExpression"] = filter_expression

        # iterate through entire table
        keepgoing = True
        batch_data = []
        while keepgoing:
            response = table.scan(**scan_kwargs)
            batch_data += response.get("Items")
            scan_kwargs["ExclusiveStartKey"] = response.get("LastEvaluatedKey")
            keepgoing = bool(scan_kwargs["ExclusiveStartKey"])
            if len(batch_data) >= batch_size or not keepgoing:
                yield batch_data
                batch_data = []
