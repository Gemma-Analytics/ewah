from ewah.operators.base_operator import EWAHBaseOperator
from ewah.ewah_utils.airflow_utils import airflow_datetime_adjustments
from ewah.constants import EWAHConstants as EC

from airflow.hooks.S3_hook import S3Hook

from datetime import datetime, timedelta

import json
import csv

class EWAHS3Operator(EWAHBaseOperator):
    """Only implemented for JSON and CSV files from S3 right now!"""

    template_fields = ('data_from', 'data_until')

    _IS_INCREMENTAL = True
    _IS_FULL_REFRESH = True

    _IMPLEMENTED_FORMATS = [
        'JSON',
        'AWS_FIREHOSE_JSON',
        'CSV',
    ]

    _BOM = b'\xef\xbb\xbf'

    def __init__(self,
        bucket_name,
        file_format,
        prefix='', # use for subfolder structures
        suffix=None, # use e.g. for file types
        key_name=None, # if not specified, uses data_from and data_until
        data_from=None,
        data_until=None,
        csv_format_options={},
        csv_encoding='utf-8',
    *args, **kwargs):

        if not file_format in self._IMPLEMENTED_FORMATS:
            raise Exception('File format not implemented! Available formats:' \
                + ' {0}'.format(str(self._IMPLEMENTED_FORMATS)))

        if key_name:
            if data_from or data_until:
                raise Exception('Both key_name and either data_from or data_' \
                    + 'until specified! Cannot have both specified.')

        if not file_format == 'CSV' and csv_format_options:
            raise Exception('csv_format_options is only valid for CSV files!')



        #if not self.drop_and_replace:
        #    if not (data_from and data_until):
        #        raise Exception('For incremental loading, you must specify ' \
        #            + 'both data_from and data_until')
        # if incrementally loading with data_from and data_until: default
        #   to execution dates!

        self.bucket_name = bucket_name
        self.prefix = prefix
        self.suffix = suffix
        self.key_name = key_name
        self.data_from = data_from
        self.data_until = data_until
        self.file_format = file_format
        self.csv_format_options = csv_format_options
        self.csv_encoding = csv_encoding

        super().__init__(*args, **kwargs)

    def ewah_execute(self, context):
        if not self.drop_and_replace and not self.key_name:
            self.data_from = self.data_from or context['execution_date']
            self.data_until = self.data_until or context['next_execution_date']

        if self.file_format == 'JSON':
            return self.execute_json(
                context=context,
                f_get_data=lambda hook, key, bucket: json.loads(
                    hook.read_key(key, bucket)
                ),
            )
        elif self.file_format == 'AWS_FIREHOSE_JSON':
            return self.execute_json(
                context=context,
                f_get_data=lambda hook, key, bucket: json.loads(
                    '['+hook.read_key(key, bucket).replace('}{', '},{')+']'
                ),
            )
        elif self.file_format == 'CSV':
            return self.execute_csv(
                context=context,
            )
        else:
            raise Exception('File format not implemented!')

    def execute_csv(self, context):
        self.data_from = airflow_datetime_adjustments(self.data_from)
        self.data_until = airflow_datetime_adjustments(self.data_until)
        hook = S3Hook(self.source_conn_id)
        suffix = self.suffix
        if self.key_name:
            data = hook.read_key(self.key_name, self.bucket_name)
            self.upload_data(data=data)
        else:
            bucket = hook.get_bucket(self.bucket_name)
            for obj in bucket.objects.filter(Prefix=self.prefix):
                # skip all files outside of loading scope
                if self.data_from and obj.last_modified < self.data_from:
                    continue
                if self.data_until and obj.last_modified >= self.data_until:
                    continue

                if suffix and not suffix == obj.key[-len(suffix):]:
                    continue

                self.log.info('Loading data from file {0}'.format(obj.key))
                raw_data = obj.get()['Body'].read()
                # remove BOM if it exists
                # also, if file has a BOM, it is 99.9% utf-9 encoded!
                if raw_data[:3] == self._BOM:
                    raw_data = raw_data[3:]
                    csv_encoding = 'utf-8'
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
                data = list(reader)
                self._metadata.update({
                    'bucket_name': self.bucket_name,
                    'file_name': obj.key,
                    'file_last_modified': str(obj.last_modified),
                })
                self.upload_data(data=data)

    def execute_json(self, context, f_get_data):
        self.data_from = airflow_datetime_adjustments(self.data_from)
        self.data_until = airflow_datetime_adjustments(self.data_until)

        hook = S3Hook(self.source_conn_id)

        if self.suffix:
            raise Exception('Feature not implemented!')

        if self.key_name:
            data = f_get_data(
                hook=hook,
                key=self.key_name,
                bucket=self.bucket_name,
            )
            self.upload_data(data=data)
        else:
            data = []
            bucket = hook.get_bucket(self.bucket_name)
            for obj in bucket.objects.filter(Prefix=self.prefix):
                if self.data_from and obj.last_modified < self.data_from:
                    continue
                if self.data_until and obj.last_modified >= self.data_until:
                    continue

                self.log.info('Loading data from file {0}'.format(
                    obj.key,
                ))
                self._metadata.update({
                    'bucket_name': self.bucket_name,
                    'file_name': obj.key,
                    'file_last_modified': str(obj.last_modified),
                })
                data = f_get_data(
                    hook=hook,
                    key=obj.key,
                    bucket=self.bucket_name,
                )
                self.upload_data(data=data)
