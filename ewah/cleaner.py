from airflow.utils.log.logging_mixin import LoggingMixin

from ewah.constants import EWAHConstants as EC

from typing import List, Dict, Optional, Any, Callable, Union

from copy import deepcopy
from hashlib import sha256

# Refactor me
from bson.json_util import dumps  # dumping mongob objects to string
from bson.objectid import ObjectId
from collections import OrderedDict
from decimal import Decimal

import json


class EWAHJSONEncoder(json.JSONEncoder):
    """Extension of the native json encoder to deal with additional datatypes and
    issues relating to (+/-) Inf and NaN numbers.
    """

    def default(self, obj):
        """Method is called if an object cannot be serialized.

        Ought to return a serializeable value for the object. Ought to raise an error
        if unable to do so.

        Implemented types:
            - Decimal -> float
            - bson.objectid.ObjectId -> string
        """

        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, ObjectId):
            return str(obj)
        # Let the base class default method raise the TypeError
        return super().default(obj)

    def iterencode(self, o, _one_shot=False):
        """Overwrite the iterencode method because this is where the float
        special cases are handled in the floatstr() function. Copy-pasted
        original code and then adapted it to change floatstr() behavior.

        This was necessary because the json module accepts (+/-) Infinity and NaN
        objects as floats, but PostgreSQL uses the tighter JSON standard and does not
        accept them in json. The json module offers no optional flag to deal with this
        issue natively. Thus, overwrite the iterencode method and remove Inf and NaN
        by returning null instead.
        """
        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = json.encoder.encode_basestring_ascii
        else:
            _encoder = json.encoder.encode_basestring

        def floatstr(
            o,
            allow_nan=self.allow_nan,
            _repr=float.__repr__,
            _inf=float("inf"),
            _neginf=-float("inf"),
        ):
            # Check for specials.  Note that this type of test is processor
            # and/or platform-specific, so do tests which don't depend on the
            # internals.
            if not (o != o or o == _inf or o == _neginf):
                return _repr(o)
            if not allow_nan:
                raise ValueError(
                    "Out of range float values are not JSON compliant: " + repr(o)
                )
            return "null"

        return json.encoder._make_iterencode(
            markers,
            self.default,
            _encoder,
            self.indent,
            floatstr,
            self.key_separator,
            self.item_separator,
            self.sort_keys,
            self.skipkeys,
            _one_shot,
        )(o, 0)


class EWAHCleaner(LoggingMixin):
    """Default data cleaner class for EWAH.

    The Cleaner is a class that bundles methods used to clean data between
    fetching and loading. Minor transformations may be executed herein.
    In addition, the Cleaner may keep track of the data schema.

    Derive from this class to extend functionalities.
    """

    def __init__(
        self,
        default_row: Optional[Dict[str, Any]] = None,
        add_metadata: bool = False,
        include_columns: Optional[List[str]] = None,
        exclude_columns: Optional[List[str]] = None,
        hash_columns: Optional[List[str]] = None,
        hash_salt: Optional[str] = None,
        rename_columns: Optional[Dict[str, str]] = None,
        additional_callables: Optional[Union[List[Callable], Callable]] = None,
        json_encoder: type = EWAHJSONEncoder,
    ):
        super().__init__()

        cleaning_steps = []

        if include_columns:
            cleaning_steps.append(self._include_columns)
            self.include_columns = include_columns

        if include_columns and exclude_columns:
            _msg = "Don't use include and exclude columns config at the same time!"
            raise Exception(_msg)

        if exclude_columns:
            cleaning_steps.append(self._exclude_columns)
            self.exclude_columns = exclude_columns

        if rename_columns:
            cleaning_steps.append(self._rename_columns)
            self.rename_columns = rename_columns

        if hash_columns:
            cleaning_steps.append(self._hash_row)
            self.hash_columns = hash_columns

        if add_metadata:
            cleaning_steps.append(self._add_metadata)
        self.add_metadata = add_metadata

        if additional_callables:
            if callable(additional_callables):
                cleaning_steps.append(additional_callables)
            else:
                cleaning_steps += additional_callables

        # Clean values right at the end
        cleaning_steps.append(self.clean_values)

        self.cleaning_steps = cleaning_steps
        self.hash_salt = hash_salt or ""
        self.default_row = default_row or {}
        self.json_encoder = json_encoder

        if default_row:
            # initialize with defaults
            self.fields_definition = {
                field: type(value) for field, value in default_row.items()
            }
        else:
            self.fields_definition = {}

    def _include_columns(self, row):
        tmp_row = {}
        for column in self.include_columns:
            if column in row:
                tmp_row[column] = row[column]
        return tmp_row

    def _exclude_columns(self, row):
        for column in self.exclude_columns:
            row.pop(column, None)
        return row

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

    def _hash_value(self, value):
        # Overwrite function for any other desired hashing behavior
        if value is None:
            return None
        return sha256((str(value) + str(self.hash_salt)).encode()).hexdigest()

    def clean_rows(
        self, rows: List[Dict[str, Any]], metadata: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        cleaned_rows = []
        self.log.info("Cleaning {0} rows of data!".format(str(len(rows))))
        if self.add_metadata:
            self.metadata = metadata or {}
        while rows:
            cleaned_rows.append(self.clean_row(rows.pop(0)))
        return cleaned_rows

    def clean_values(self, raw_row: dict):
        row = deepcopy(self.default_row)
        # Note: Destructive iteration using raw_row.popitem() causes a reversed order!
        # --> create a list of tuples first and then destructively iterate over it.
        key_value_pairs = list(raw_row.items())
        while key_value_pairs:
            key, value = key_value_pairs.pop(0)
            if not value is None:
                value_type = None  # May be set during the type change below
                if isinstance(value, str):
                    if value == "\0":
                        # This is a null value -> treat as None
                        value = row.get(key)  # Use default, if exists
                    else:
                        # Some database systems don't handle this character well
                        # Thus, remove it
                        value = value.replace("\x00", "")
                elif isinstance(value, (dict, OrderedDict, list)):
                    if isinstance(value, OrderedDict):
                        value_type = dict
                    else:
                        value_type = type(value)
                    # Logic copy-pasted from legacy - TODO: Refactor this!
                    try:
                        value = json.dumps(value, cls=self.json_encoder)
                    except TypeError:
                        # try dumping with bson utility function
                        # Refactor this, PLEASE!
                        value = dumps(value)
                elif isinstance(value, Decimal):
                    value = float(value)
                row[key] = value

                # Set the fields_definition for the key
                value_type = value_type or type(value)  # set now if not done above
                current_type_set = self.fields_definition.get(key)
                if current_type_set:
                    if (
                        not current_type_set == value_type
                        and not current_type_set == str
                    ):
                        # TODO: make this flexible
                        # For now, default to text in case of conflict
                        self.fields_definition[key] = str
                        self.log.info(
                            "\n\nWARNING! Data types are inconsistent.\n"
                            "\tAffected field: {0}\n"
                            "\tFirst type: {1}\n"
                            "\tSecond type: {2}\n".format(
                                key, str(current_type_set), str(value_type)
                            )
                        )
                else:
                    self.fields_definition[key] = value_type

        return row

    def clean_row(self, row: dict):
        for step in self.cleaning_steps:
            row = step(row)
        return row

    def get_columns_definition(self, dwh_engine):
        columns_definition = {}
        for field, datatype in self.fields_definition.items():
            data_type = EC.QBC_TYPE_MAPPING[dwh_engine].get(datatype)
            if not data_type:
                raise Exception(
                    "Field '{field}' has an invalid data type '{data_type}'!".format(
                        field=field, data_type=str(datatype)
                    )
                )
            columns_definition[field] = {EC.QBC_FIELD_TYPE: data_type}
        return columns_definition
