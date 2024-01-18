from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from io import StringIO
from multiprocessing.dummy import Pool as ThreadPool

import json
import csv
import gzip
import time


class ExtendedS3Hook(S3Hook):
    """Extends and improves the provider package's S3Hook's capability"""

    credentials_by_region = {}
    force_refresh = False
    is_refreshing = {}

    def _get_credentials(self, region_name, force_refresh=False):
        "Avoid creation of a new session each time an S3 file is downloaded."
        if (not self.is_refreshing.get(region_name)) and (
            force_refresh
            or self.force_refresh
            or (not region_name in self.credentials_by_region)
        ):
            # Avoid all threads renewing credentials at the same time
            self.is_refreshing[region_name] = True
            self.force_refresh = False
            self.credentials_by_region[region_name] = super()._get_credentials(
                region_name=region_name
            )
            self.is_refreshing[region_name] = False
        while self.is_refreshing.get(region_name):
            # Wait until the appropriate thread is finished refreshing credentials
            time.sleep(0.1)
        return self.credentials_by_region[region_name]

    def read_key(self, key, bucket_name=None, is_compressed=False):
        """
        Overwrite method in order to allow for compressed objects
        Source: https://airflow.apache.org/docs/apache-airflow/1.10.12/_api/airflow/hooks/S3_hook/index.html#airflow.hooks.S3_hook.S3Hook.read_key
        """
        obj = self.get_key(key, bucket_name)
        if is_compressed:
            return obj.get()["Body"].read()
        return obj.get()["Body"].read().decode("utf-8")


class EWAHS3Operator(EWAHBaseOperator):
    """Only implemented for JSON and CSV files from S3 right now!"""

    _NAMES = ["s3"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _IMPLEMENTED_FORMATS = [
        "JSON",
        "JSONL",  # Multiple JSON objects split by newlines in the same file
        "AWS_FIREHOSE_JSON",
        "CSV",
    ]

    _BOM = b"\xef\xbb\xbf"

    def __init__(
        self,
        bucket_name,
        file_format,
        prefix="",  # use for subfolder structures
        suffix=None,  # use e.g. for file types
        key_name=None,  # if not specified, uses data_from and data_until
        csv_format_options={},
        csv_encoding="utf-8",
        decompress=False,
        file_load_parallelism=1000,
        thread_pool_size=20,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        if not file_format in self._IMPLEMENTED_FORMATS:
            raise Exception(
                "File format not implemented! Available formats:"
                + " {0}".format(str(self._IMPLEMENTED_FORMATS))
            )

        if key_name:
            if self.load_data_from or self.load_data_until:
                raise Exception(
                    "Both key_name and either data_from or data_"
                    + "until specified! Cannot have both specified."
                )

        if not file_format == "CSV" and csv_format_options:
            raise Exception("csv_format_options is only valid for CSV files!")

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.suffix = suffix
        self.key_name = key_name
        self.file_format = file_format
        self.csv_format_options = csv_format_options
        self.csv_encoding = csv_encoding
        self.decompress = decompress
        self.file_load_parallelism = file_load_parallelism
        self.thread_pool_size = thread_pool_size

    def _iterate_through_bucket(
        self,
        s3hook,
        bucket,
        prefix,
        modified_from=None,
        modified_until=None,
        suffix=None,
        file_load_parallelism=1000,
        thread_pool_size=20,
    ):
        """The bucket.objects.filter() method only returns a max of 1000
        objects. If more objects are in an S3 bucket, pagniation is
        required. See also: https://stackoverflow.com/questions/44238525/how-to-iterate-over-files-in-an-s3-bucket
        """

        def read_key(key_name):
            return s3hook.read_key(key_name, bucket, self.decompress)

        cli = s3hook.get_client_type("s3")
        paginator = cli.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

        all_objects = [
            o
            for o in page_iterator.build_full_result()["Contents"]
            if (not modified_from or o["LastModified"] > modified_from)
            and (not modified_until or o["LastModified"] <= modified_until)
            and (not suffix or suffix == o["Key"][-len(suffix) :])
        ]

        self._temp_object_data = {}

        list_of_keys = [o["Key"] for o in all_objects]
        self.log.info("Iterating through {0} objects..".format(len(all_objects)))

        for item in all_objects:
            if not item["Key"] in self._temp_object_data:
                # use multithreading to efficiently load s3 files
                self.log.info(
                    "Getting next {0} files...".format(str(file_load_parallelism))
                )
                _next_keys = list_of_keys[:file_load_parallelism]
                del list_of_keys[:file_load_parallelism]
                pool = ThreadPool(thread_pool_size)
                try:
                    results = pool.map(read_key, _next_keys)
                except ClientError:
                    # This error can occur when the aws token expires - try refreshing
                    # the connection and see if the error persists
                    s3hook.force_refresh = True
                    results = pool.map(read_key, _next_keys)
                pool.close()
                pool.join()
                self._temp_object_data.update(dict(zip(_next_keys, results)))
            item["_body"] = self._temp_object_data.pop(item["Key"])
            yield item

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            self.data_from = self.get_max_value_of_column(self.subsequent_field)
        if self.file_format == "JSON":
            return self.execute_json(
                context=context,
                f_get_data=lambda file_content: json.loads(file_content),
            )
        elif self.file_format == "JSONL":
            f_get_data = lambda file_content: list(
                # Use StringIO in order to iterate through each line of the file
                map(json.loads, StringIO(file_content))
            )
            if self.decompress:
                f_get_data = lambda file_content: list(
                    map(
                        json.loads,
                        # We need to decompress the whole file before iterating over its lines
                        StringIO(gzip.decompress(bytes(file_content)).decode("utf-8")),
                    )
                )
            return self.execute_json(
                context=context,
                f_get_data=f_get_data,
            )
        elif self.file_format == "AWS_FIREHOSE_JSON":
            return self.execute_json(
                context=context,
                f_get_data=lambda file_content: json.loads(
                    "["  # hotfix for a weird encoding issue
                    + file_content.replace("\x00", "")
                    .replace("\u0000", "")
                    .replace("\\u0000", "")
                    .replace("\\x00", "")
                    .replace("}{", "},{")
                    + "]"
                ),
            )
        elif self.file_format == "CSV":
            return self.execute_csv(
                context=context,
            )
        else:
            raise Exception("File format not implemented!")

    def execute_csv(self, context):
        def chunk_upload(csv_reader):
            i = 0
            data = []
            for row in csv_reader:
                i += 1
                data.append(row)
                if i >= 100000:
                    self.upload_data(data=data)
                    data = []
                    i = 0
            if data:
                self.upload_data(data=data)

        hook = ExtendedS3Hook(self.source_conn.conn_id)
        if self.key_name:
            obj = hook.get_key(self.key_name, self.bucket_name)
            raw_data = obj.get()["Body"].read()
            if self.decompress:
                raw_data = gzip.decompress(raw_data)
            if raw_data[:3] == self._BOM:
                raw_data = raw_data[3:]
                csv_encoding = "utf-8"
            else:
                csv_encoding = self.csv_encoding
            try:
                raw_data = raw_data.decode(csv_encoding).splitlines()
            except UnicodeDecodeError:
                if csv_encoding == self.csv_encoding:
                    raise
                raw_data = raw_data.decode(self.csv_encoding).splitlines()
            reader = csv.DictReader(raw_data, **self.csv_format_options)
            chunk_upload(reader)

        else:
            for obj_iter in self._iterate_through_bucket(
                s3hook=hook,
                bucket=self.bucket_name,
                prefix=self.prefix,
                modified_from=self.data_from,
                modified_until=self.data_until,
                suffix=self.suffix,
                file_load_parallelism=self.file_load_parallelism,
                thread_pool_size=self.thread_pool_size,
            ):
                self.log.info("Loading data from file {0}".format(obj_iter["Key"]))
                raw_data = obj_iter["_body"]
                if self.decompress:
                    raw_data = gzip.decompress(raw_data)
                # remove BOM if it exists
                # also, if file has a BOM, it is 99.9% utf-8 encoded!
                if raw_data[:3] == self._BOM:
                    raw_data = raw_data[3:]
                    csv_encoding = "utf-8"
                else:
                    csv_encoding = self.csv_encoding
                # there may be a BOM while still not utf-8 -> use the
                # given csv_encoding argument if so
                try:
                    raw_data = raw_data.decode(csv_encoding).splitlines()
                except UnicodeDecodeError:
                    if csv_encoding == self.csv_encoding:
                        raise
                    raw_data = raw_data.decode(self.csv_encoding).splitlines()
                reader = csv.DictReader(raw_data, **self.csv_format_options)
                if self.subsequent_field:
                    # TODO: Abstract this logic
                    for datum in reader:
                        datum[self.subsequent_field] = datetime.strptime(
                            datum[self.subsequent_field], "%Y-%m-%dT%H:%M:%S.%f%z"
                        )
                self._metadata.update(
                    {
                        "bucket_name": self.bucket_name,
                        "file_name": obj_iter["Key"],
                        "file_last_modified": str(obj_iter["LastModified"]),
                    }
                )
                chunk_upload(reader)

    def execute_json(self, context, f_get_data):
        hook = ExtendedS3Hook(self.source_conn.conn_id)

        if self.key_name:
            data = f_get_data(
                file_content=hook.read_key(key=self.key_name, bucket=self.bucket_name)
            )
            self.upload_data(data=data)
        else:
            data = []
            objects = self._iterate_through_bucket(
                s3hook=hook,
                bucket=self.bucket_name,
                prefix=self.prefix,
                modified_from=self.data_from,
                modified_until=self.data_until,
                suffix=self.suffix,
                file_load_parallelism=self.file_load_parallelism,
                thread_pool_size=self.thread_pool_size,
            )
            for obj_iter in objects:
                self.log.info(
                    "Loading data from file {0}".format(
                        obj_iter["Key"],
                    )
                )
                self._metadata.update(
                    {
                        "bucket_name": self.bucket_name,
                        "file_name": obj_iter["Key"],
                        "file_last_modified": obj_iter["LastModified"],
                    }
                )
                data = f_get_data(file_content=obj_iter["_body"])
                if self.subsequent_field:
                    # TODO: Abstract this logic
                    for datum in data:
                        # Avoid KeyError when using `file_last_modified` as subsequent field
                        if not self.subsequent_field in self._metadata.keys():
                            datum[self.subsequent_field] = datetime.strptime(
                                datum[self.subsequent_field], "%Y-%m-%dT%H:%M:%S.%f%z"
                            )
                self.upload_data(data=data)
