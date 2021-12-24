from airflow.utils.log.logging_mixin import LoggingMixin

from typing import List, Dict, Optional, Any, Callable, Union

from copy import deepcopy
from hashlib import sha256


class EWAHCleaner(LoggingMixin):
    """Default data cleaner class for EWAH.

    The Cleaner is a class that bundles methods used to clean data between
    fetching and loading. Minor transformations may be executed herein.
    In addition, the Cleaner may keep track of the data schema.

    Derive from this class to extend functionalities.
    """

    def __init__(
        self,
        default_row: Optional[Dict[str, any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        rename_columns: Optional[Dict[str, str]] = None,
        hash_columns: Optional[List[str]] = None,
        additional_callables: Optional[Union[List[Callable], Callable]] = None,
    ):
        super().__init__()

        cleaning_steps = []

        if metadata:
            cleaning_steps.append(self._add_metadata)
            self.metadata = metadata

        if rename_columns:
            cleaning_steps.append(self._rename_columns)
            self.rename_columns = rename_columns

        if hash_columns:
            cleaning_steps.append(self._hash_row)
            self.hash_columns = hash_columns

        if additional_callables:
            if callable(additional_callables):
                cleaning_steps.append(additional_callables)
            else:
                cleaning_steps += additional_callables

        self.cleaning_steps = cleaning_steps
        self.default_row = default_row or {}

    def _add_metadata(self, row):
        row.update(self.metadata)
        return row

    def _rename_columns(self, row):
        for (old_name, new_name) in self.rename_columns.items():
            row[new_name] = row.pop(old_name, row.get(new_name))
        return row

    def _hash_row(self, row):
        for column in self.hash_columns:
            row[column] = self._hash_value(row.get(column))
        return row

    @staticmethod
    def _hash_value(value):
        # Overwrite function for any other desired hashing behavior
        if value is None:
            return None
        return sha256(str(value).encode()).hexdigest()

    def clean_rows(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        cleaned_rows = []
        self.log.info("Cleaning {0} rows of data!".format(str(len(rows))))
        while rows:
            cleaned_rows.append(self.clean_row(rows.pop(0)))
        return cleaned_rows

    def clean_row(self, raw_row: dict):
        row = deepcopy(self.default_row)

        while raw_row:
            key, value = raw_row.popitem()
            if not value is None:
                if isinstance(value, str) and not (value == "\0"):
                    # Some database system don't handle this character well, remove it
                    value = value.replace("\x00", "")
                row[key] = value

        for step in self.cleaning_steps:
            row = step(row)

        return row
