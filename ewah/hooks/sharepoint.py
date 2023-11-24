from ewah.hooks.base import EWAHBaseHook

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

from openpyxl import load_workbook

import io


class EWAHSharepointHook(EWAHBaseHook):
    _ATTR_RELABEL: dict = {
        "user": "login",
        "site_url": "schema",
    }

    conn_name_attr: str = "ewah_sharepoint_conn_id"
    default_conn_name: str = "ewah_sharepoint_default"
    conn_type: str = "ewah_sharepoint"
    hook_name: str = "EWAH Microsoft Sharepoint Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["port", "extra", "host"],
            "relabeling": {
                "password": "Password",
                "login": "User",
                "schema": "Sharepoint Site URL",
            },
        }

    def get_data_from_excel(
        self,
        relative_url: str,
        worksheet_name: str,
        header_row: int,
        start_row: int,
        batch_size: int,
    ):
        # adapted from: https://stackoverflow.com/a/69292234/14125255
        # (accessed 2021-10-15)

        assert (
            isinstance(batch_size, int) and batch_size > 0
        ), f"type: {type(batch_size)}, value: {str(batch_size)}"
        assert (
            isinstance(header_row, int) and header_row > 0
        ), f"type: {type(header_row)}, value: {str(header_row)}"
        assert (
            isinstance(start_row, int) and start_row > 0
        ), f"type: {type(start_row)}, value: {str(start_row)}"
        assert start_row > header_row

        ctx = ClientContext(self.conn.site_url).with_credentials(
            UserCredential(self.conn.user, self.conn.password)
        )
        response = File.open_binary(ctx, relative_url)
        bytes_file_obj = io.BytesIO()
        bytes_file_obj.write(response.content)
        bytes_file_obj.seek(0)
        ws = load_workbook(bytes_file_obj, data_only=True)[worksheet_name]
        headers = {
            ws.cell(row=header_row, column=col).value: col
            for col in range(1, ws.max_column + 1)
            if ws.cell(row=header_row, column=col)
        }
        while start_row < ws.max_row + 1:
            yield [
                {k: ws.cell(row=row, column=v).value for k, v in headers.items()}
                for row in range(start_row, min(start_row + batch_size, ws.max_row + 1))
            ]
            start_row += batch_size
