from ewah.operators.base import EWAHBaseOperator
from ewah.constants import EWAHConstants as EC
from ewah.hooks.sharepoint import EWAHSharepointHook


class EWAHSharepointOperator(EWAHBaseOperator):
    _NAMES = ["sharepoint"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: False,
    }

    _CONN_TYPE = EWAHSharepointHook.conn_type

    def __init__(
        self,
        file_relative_path: str,
        sheet_name: str,
        header_row: int = 1,
        start_row: int = 2,
        batch_size: int = 10000,
        *args,
        **kwargs,
    ) -> None:
        assert file_relative_path.endswith((".xlsx", ".xlsm")), "Only Excel files for now!"
        self.file_relative_path = file_relative_path
        self.sheet_name = sheet_name
        self.header_row = header_row
        self.start_row = start_row
        self.batch_size = batch_size
        super().__init__(*args, **kwargs)

    def ewah_execute(self, context: dict) -> None:
        self.log.info("Loading data...")
        for batch in self.source_hook.get_data_from_excel(
            self.file_relative_path,
            self.sheet_name,
            self.header_row,
            self.start_row,
            self.batch_size,
        ):
            self.upload_data(batch)
