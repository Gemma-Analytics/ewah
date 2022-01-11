from ewah.uploaders.base import EWAHBaseUploader
from ewah.constants import EWAHConstants as EC

from airflow.operators.dummy import DummyOperator

from tempfile import NamedTemporaryFile
from datetime import datetime
from decimal import Decimal
import gspread
import json
import os


def monkeypatch_values_update(func_to_call):
    def monkeypatch_func(range_name, params, body):
        values = body.get("values")
        if values:
            values = [
                [
                    float(v)
                    if isinstance(v, Decimal)
                    else (
                        v.strftime("%Y-%m-%d %H:%M:%S%z")
                        if isinstance(v, datetime)
                        else v
                    )
                    for v in outer
                ]
                for outer in values
            ]
            body["values"] = values
        return func_to_call(range_name, params=params, body=body)

    return monkeypatch_func


class EWAHGSheetsUploader(EWAHBaseUploader):
    """
    The Google Sheets Hook works a little bit different to other hooks.
        - it can only have 1 upload call per task execution
        - it deletes all data from the sheet and re-uploads it to the sheet
        - the target schema name is the workbook key
        - the target table name is the worksheet name
    """

    def __init__(self, *args, **kwargs):
        self._upload_call_count = 0  # Make sure there is only a single upload
        super().__init__(EC.DWH_ENGINE_GS, *args, **kwargs)

    @classmethod
    def get_schema_tasks(cls, dag, *args, **kwargs):
        # Dummy tasks - there are no schemas in Google Sheets
        return (
            DummyOperator(
                task_id="kickoff",
                dag=dag,
            ),
            DummyOperator(
                task_id="final",
                dag=dag,
            ),
        )

    def detect_and_apply_schema_changes(self, *args, **kwargs):
        # need to overwrite parent function, does nothing
        raise Exception("This function is not implemented for this DWHook!")

    def commit(self):
        self.log.info("Everything is auto-committed")

    def rollback(self):
        raise Exception("Cannot rollback Google Sheets target!")

    def close(self):
        self.log.info("Nothing to close")

    def _create_or_update_table(
        self,
        data,
        table_name,
        schema_name,
        schema_suffix,  # unused but always given
        columns_definition,
        load_strategy,  # Must be LS_INSERT_REPLACE
        upload_call_count,  # Must be 1
        primary_key=None,  # must accept arg, but it must also always be []
    ):
        # Google Sheets only works with drop & replace in one go
        assert (
            load_strategy == EC.LS_INSERT_REPLACE
        ), "Google Sheets DWHs can only be drop_and_replace!"
        assert upload_call_count == 1, "Chunking is not possible for Google Sheets DWH!"

        if primary_key:
            raise Exception("Arg primary_key invalidly supplied!")

        def colnum_string(n):
            # adapted from https://stackoverflow.com/questions/23861680/convert-spreadsheet-number-to-column-letter
            string = ""
            while n > 0:
                n, remainder = divmod(n - 1, 26)
                string = chr(65 + remainder) + string
            return string

        self.log.info("Replacing data in Google Sheets!")

        if not data:
            self.log.info("Nothing to upload!")
            return

        # authorize and get correct worksheet
        servie_acc_file = NamedTemporaryFile()
        self.log.info("Authenticating...")
        credentials = self.dwh_conn.extra_dejson
        credentials = credentials.get("client_secrets", credentials)
        service_acc = json.dumps(credentials)
        filename = os.path.abspath(servie_acc_file.name)
        with open(filename, "w") as f:
            f.write(service_acc)
        gclient = gspread.service_account(filename=filename)

        workbook = gclient.open_by_key(schema_name)
        worksheet = workbook.worksheet(table_name)
        values = worksheet.get_all_values()

        # Delete old data, if any existed
        if values:
            self.log.info("Deleting old data...")
            rows = len(values)
            columns = len(values[0])
            values = [columns * [""]] * rows
            range_notation = "A1:" + colnum_string(columns) + str(rows)
            worksheet.update(range_notation, values)  # delete!

        del values  # free up memory

        # insert new data - need to change the format! use columns definition
        self.log.info("Preparing data for Google Sheets upload...")
        column_header = list(columns_definition.keys())
        upload_data = [column_header]  # First row: headers
        while data:
            current_data = data.pop(0)
            upload_data.append([current_data.get(col, "") for col in column_header])
        range_notation = "A1:" + colnum_string(len(column_header))
        range_notation += str(len(upload_data))
        self.log.info("Uploading data now!")
        worksheet.spreadsheet.values_update = monkeypatch_values_update(
            worksheet.spreadsheet.values_update
        )
        worksheet.update(range_notation, upload_data)
        self.log.info("Upload done.")
