from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC

from ewah.hooks.base import EWAHBaseHook as BaseHook

import gspread
import json
from oauth2client.service_account import ServiceAccountCredentials as SAC


class EWAHGSpreadOperator(EWAHBaseOperator):

    _NAMES = ["google_sheets", "gs", "gsheets"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _SAMPLE_JSON = {
        "client_secrets": {
            "type": "service_account",
            "project_id": "abc-123",
            "private_key_id": "123456abcder",
            "private_key": "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----\n",
            "client_email": "xyz@abc-123.iam.gserviceaccount.com",
            "client_id": "123457",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xyz%40abc-123.iam.gserviceaccount.com",
        }
    }

    @staticmethod
    def _translate_alphanumeric_column(column_identifier):
        if isinstance(column_identifier, str):
            column_number = 0
            i = 0
            ident_dict = {}
            while column_identifier:
                letter = column_identifier[-1:].lower()
                if ord(letter) > ord("z") or ord(letter) < ord("a"):
                    raise Exception(
                        "Column letter {0} out of bounds!".format(
                            letter,
                        )
                    )
                column_identifier = column_identifier[:-1]
                ident_dict.update({i: ord(letter) + 1 - ord("a")})
                i += 1
            return sum([v * (26 ** k) for k, v in ident_dict.items()])
        else:
            return column_identifier

    def __init__(
        self,
        workbook_key,  # can be seen in the URL of the workbook
        sheet_key,  # name of the worksheet
        sheet_columns,  # list or dict[column name, position] defining which columns to load
        start_row=2,  # in what row does the data begin?
        end_row=None,  # optional: what is the last row? None gets all data
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        credentials = BaseHook.get_connection(self.source_conn_id).extra_dejson
        credentials = credentials.get("client_secrets", credentials)

        _msg = "Google Service Account Credentials misspecified!"
        _msg += " Example of a correct specifidation: {0}".format(
            json.dumps(self._SAMPLE_JSON)
        )
        for key in self._SAMPLE_JSON["client_secrets"]:
            if not key in credentials:
                raise Exception(_msg)

        column_match = {}
        if isinstance(sheet_columns, list):
            i = 0
            for column in sheet_columns:
                i += 1
                column_match[i] = column
        elif isinstance(sheet_columns, dict):
            column_match = {
                self._translate_alphanumeric_column(value): key
                for key, value in sheet_columns.items()
            }
        else:
            raise Exception("sheet_columns must be a list or a dict!")

        self.client_secrets = credentials
        self.column_match = column_match
        self.workbook_key = workbook_key
        self.sheet_key = sheet_key
        self.start_row = start_row
        self.end_row = end_row

    def ewah_execute(self, context):
        client = gspread.authorize(
            SAC.from_json_keyfile_dict(
                self.client_secrets,
                ["https://spreadsheets.google.com/feeds"],
            ),
        )

        self.log.info("Retrieving data...")
        workbook = client.open_by_key(self.workbook_key)
        sheet = workbook.worksheet(self.sheet_key)
        raw_data = sheet.get_all_values()[self.start_row - 1 : self.end_row]

        # Load the data from the sheet into a format for upload into the DWH
        data = []
        for row in raw_data:  # Iterate through each row
            data_dict = {}  # New row, new dictionary of field:value
            row_is_null = True  # Is entire row empty?
            for position, column in self.column_match.items():
                datapoint = row[position - 1]
                data_dict.update({column: datapoint})
                if row_is_null:
                    if bool(datapoint):
                        if not (datapoint == "0"):
                            # Special case with zeroes in text format
                            row_is_null = False  # Row is not empty!
            if not row_is_null:  # Ignore empty rows
                data.append(data_dict)

        self.upload_data(data)
