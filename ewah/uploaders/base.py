from airflow.utils.log.logging_mixin import LoggingMixin

from ewah.hooks.base import EWAHBaseHook
from ewah.constants import EWAHConstants as EC

import json
import bson
import math
import hashlib

from copy import deepcopy
from collections import OrderedDict
from psycopg2.extras import RealDictCursor
from bson.json_util import dumps
from decimal import Decimal
from typing import Optional, Type, Union, Dict


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
        if isinstance(obj, bson.objectid.ObjectId):
            # MongoDB Object IDs - return just the ID itself as string
            return str(obj)
        if isinstance(obj, Decimal):
            return float(obj)
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


class EWAHBaseUploader(LoggingMixin):
    """Base class for all EWAH uploader classes aka Uploaders.

    Uploaders are classes that can receive data in a standardized format and upload
    it to a target data storage. Each Uploader is uploads data to one type of target.
    For instance, the EWAHPostgresUploader loads data to PostgreSQL databases.

    The base class contains the basic methods shared by all Uploaders for the purpose of
    uploading data. Each target shall have a subclass derived from this class.
    """

    def __init__(
        self, dwh_engine: str, dwh_conn: Type[EWAHBaseHook], *args, **kwarg
    ) -> None:
        super().__init__(*args, **kwarg)
        self.dwh_engine = dwh_engine
        self.dwh_conn = dwh_conn
        self.dwh_hook = dwh_conn.get_hook()

    def _get_column_type(self, column_definition: dict) -> str:
        """Return the column type of the field. Returns the DWH engine specific default
        if there is none.

        :param column_definition: Dictionary containing details on this column,
            including optionally the column type.
        """
        return column_definition.get(
            EC.QBC_FIELD_TYPE,
            EC.QBC_TYPE_MAPPING[self.dwh_engine].get(
                EC.QBC_TYPE_MAPPING_DEFAULT,
                EC.QBC_TYPE_MAPPING_INCONSISTENT,
            ),
        )

    def close(self):
        self.dwh_hook.close()

    def copy_table(
        self,
        old_schema: str,
        old_table: str,
        new_schema: str,
        new_table: str,
        **kwargs: Optional[Dict[str, str]],  # e.g. database_name
    ) -> None:
        """Copy a table, including all data.

        :param old_schema: Schema name of the source table.
        :param old_table: Table name of the source table.
        :param new_schema: Schema name of the newly created table.
        :param new_table: Table name of the newly created table.

        :Keyword Arguments:
            * Any argument that may be given as format keyword argument to format the
                ``_COPY_TABLE`` default SQL, e.g. ``database_name`` for Snowflake
        """
        test_kwargs = {"table_name": old_table, "schema_name": old_schema}
        test_kwargs.update(
            {key: value for (key, value) in kwargs.items() if not value is None}
        )
        if self.test_if_table_exists(**test_kwargs):
            self.dwh_hook.execute(
                sql=self._COPY_TABLE.format(
                    old_schema=old_schema,
                    old_table=old_table,
                    new_schema=new_schema,
                    new_table=new_table,
                    **kwargs,
                ),
                commit=False,
            )

    def detect_and_apply_schema_changes(
        self,
        new_schema_name,
        new_table_name,
        new_columns_dictionary,
        drop_missing_columns=False,
        database=None,
        commit=False,
    ):
        params = {
            "schema_name": new_schema_name,
            "table_name": new_table_name,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            database = database or self.database or self.dwh_hook.conn.database
            params["database_name"] = database
        if not self.test_if_table_exists(**params):
            return ([], [])  # Table did not previously exist, so there is nothing to do

        list_of_old_columns = [
            col[0].strip()
            for col in self.dwh_hook.execute_and_return_result(
                sql=self._QUERY_SCHEMA_CHANGES_COLUMNS.format(**params),
                params=params,
                return_dict=False,
            )
        ]
        list_of_columns = [
            col_name.strip() for col_name in list(new_columns_dictionary.keys())
        ]

        dropped_columns = []
        if drop_missing_columns:
            for old_column in list_of_old_columns:
                if not (old_column in list_of_columns):
                    drop_params = deepcopy(params)
                    drop_params["column_name"] = old_column
                    dropped_columns += [old_column]
                    self.dwh_hook.execute(
                        sql=self._QUERY_SCHEMA_CHANGES_DROP_COLUMN.format(
                            **drop_params,
                        ),
                        commit=False,
                    )

        new_columns = []
        for column in list_of_columns:
            if not (column in list_of_old_columns):
                new_columns += [column]
                add_params = deepcopy(params)
                add_params["column_name"] = column
                add_params["column_type"] = self._get_column_type(
                    new_columns_dictionary[column]
                )
                self.dwh_hook.execute(
                    sql=self._QUERY_SCHEMA_CHANGES_ADD_COLUMN.format(
                        **add_params,
                    ),
                    commit=False,
                )

        if commit:
            self.dwh_hook.commit()

        return (new_columns, dropped_columns)

    def create_or_update_table(
        self,
        data,
        columns_definition,
        table_name,
        schema_name,
        schema_suffix,
        database_name=None,
        drop_and_replace=True,
        update_on_columns=None,
        commit=False,
        clean_data_before_upload=True,
        hash_columns=None,
        hashlib_func_name=None,
    ):
        # check this again with Snowflake!!
        database_name = database_name or getattr(self, "database", None)

        mapped_types = tuple(
            [
                k
                for k in EC.QBC_TYPE_MAPPING[self.dwh_engine].keys()
                if isinstance(k, type)
            ]
        )

        raw_row = {}  # Used as template for params at execution
        sql_part_columns = []  # Used for CREATE and INSERT / UPDATE query
        jsonb_columns = []  # JSON columns require special treatment
        create_update_on_columns = not (drop_and_replace or update_on_columns)
        update_on_columns = update_on_columns or []
        if not isinstance(update_on_columns, list):
            raise Exception(
                '"update_on_columns" must be a list!'
                + "Is currently: type {0}".format(str(type(update_on_columns)))
            )
        field_constraints_mapping = EC.QBC_FIELD_CONSTRAINTS_MAPPING.get(
            self.dwh_engine,
        )

        # don't add primary key to create table definition
        # instead make alter table call later for the case of composite PKs
        field_constraints_mapping.pop(EC.QBC_FIELD_PK, None)
        pk_columns = []

        hash_columns = hash_columns or []
        for column_name in columns_definition.keys():
            raw_row[column_name] = None
            definition = columns_definition[column_name]
            if not isinstance(definition, dict):
                raise Exception(
                    "Column {0} is not properly defined!".format(
                        column_name,
                    )
                )
            sql_part_columns += [
                '"{0}"\t{1}\t{2}'.format(
                    column_name,  # Field name
                    self._get_column_type(definition),  # Type: text, int etc.
                    " ".join(
                        [  # Get all additional field properties (unique etc.)
                            field_constraints_mapping[addon]
                            if definition.get(addon)
                            else ""
                            for addon in list(field_constraints_mapping.keys())
                        ]
                    ),
                )
            ]
            if self._get_column_type(definition) == "jsonb":
                jsonb_columns += [column_name]
            if definition.get(EC.QBC_FIELD_PK):
                pk_columns += [column_name]
                if create_update_on_columns:
                    update_on_columns += [column_name]
            if definition.get(EC.QBC_FIELD_HASH):
                hash_columns += [column_name]
        sql_part_columns = ",\n\t".join(sql_part_columns)

        if clean_data_before_upload:
            self.log.info("Cleaning data for upload...")
            upload_data = []
            cols_list = list(raw_row.keys())
            if hash_columns:
                hash_func = getattr(hashlib, hashlib_func_name)
            while data:
                datum = data.pop(0)
                # Make sure that each dict in upload_data has all keys
                row = deepcopy(raw_row)
                for column_name, value in datum.items():
                    if column_name in cols_list:
                        if column_name in hash_columns:
                            # hash column!
                            pre_digest = hash_func(str(value).encode())
                            row[column_name] = pre_digest.hexdigest()
                        elif not value is None:
                            # avoid edge case of data where all instances of a
                            #   field are None, thus having data for a field
                            #   missing in the columns_definition!
                            if (
                                column_name in jsonb_columns
                                or isinstance(value, (dict, OrderedDict, list))
                                or (not isinstance(value, mapped_types))
                            ):
                                try:
                                    row[column_name] = json.dumps(
                                        value, cls=EWAHJSONEncoder
                                    )
                                except TypeError:
                                    # try dumping with bson utility function
                                    row[column_name] = dumps(value)
                            elif isinstance(value, str):
                                if not (value == "\0"):
                                    row[column_name] = value.replace("\x00", "")
                            else:
                                row[column_name] = value
                upload_data += [row]
            data_len = len(upload_data)
        else:
            data_len = len(data)
            upload_data = None

        self.log.info(
            "Uploading {0} rows of data...".format(
                str(data_len),
            )
        )

        kwargs = {}
        if database_name:
            kwargs["database_name"] = database_name

        kwargs.update(
            {
                "data": upload_data or data,
                "table_name": table_name,
                "schema_name": schema_name,
                "schema_suffix": schema_suffix,
                "columns_definition": columns_definition,
                "columns_partial_query": sql_part_columns,
                "update_on_columns": update_on_columns,
                "drop_and_replace": drop_and_replace,
                "pk_columns": pk_columns,
            }
        )
        self._create_or_update_table(**kwargs)

        if commit:
            self.dwh_hook.commit()
