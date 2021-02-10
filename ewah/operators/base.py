from airflow.models import BaseOperator

from ewah.constants import EWAHConstants as EC
from ewah.ewah_utils.airflow_utils import (
    datetime_utcnow_with_tz,
    airflow_datetime_adjustments as ada,
)
from ewah.hooks.base import EWAHBaseHook
from ewah.uploaders import get_uploader

from datetime import datetime, timedelta
from typing import Optional, List, Dict

import copy
import hashlib
import time


class EWAHBaseOperator(BaseOperator):
    """Extension of airflow's native Base Operator.

    EWAH operators always work in the same way: They extract raw data from a
    source and load it into a relational database aka data warehouse aka DWH.
    Transformations only happen insofar as they are required to bring data
    into a relational format.

    *How to use*
    The child class extracts data from the source. At the end of the child
    class' execute() function, it calls self.update(). Optional: The child can
    also repeatedly call the self.update() class for chunking.

    *Arguments of self.update():*
    Data is a list of dictionaries, where each list element will become
    one row of data and the contained dict is fieldname:value. Value can be
    None, or a fieldname can be missing in a particular dictionary, in which
    case the value is assumed to be None.

    columns_definition may be supplied at the self.update() function call,
    at initialization, or not at all, depending on the usecase. The use order is
    - If available, take the columns_definition from the self.update() call
    - If None, check if columns_definition was supplied to the operator at init
    - If neither, create a columns_definition on the fly using (all) the data

    columns_definition is a dictionary with fieldname:properties. Properties
    is None or a dictionary of option:value where option can be one of the
    following:
    - EC.QBC_FIELD_TYPE -> String: Field type (default text)
    - EC.QBC_FIELD_PK -> Boolean: Is this field the primary key? (default False)

    Note that the value of EC.QBC_FIELD_TYPE is DWH-engine specific!

    columns_definition is case sensitive.

    Implemented DWH engines:
    - PostgreSQL
    - Snowflake

    Next up:
    - BigQuery
    - Redshift
    """

    # Overwrite _NAMES with a list of names that work as aliases when defining
    # DAGs via YAML
    _NAMES = []

    # in child classes, don't overwrite this but add values within __init__!
    template_fields = (
        "load_data_from",
        "load_data_until",
    )

    # Child class must update or overwrite these values
    # A missing element is interpreted as False
    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: False,
        EC.ES_SUBSEQUENT: False,
    }

    _REQUIRES_COLUMNS_DEFINITION = False  # raise error if true and None supplied

    _CONN_TYPE = None  # overwrite me with the required connection type, if applicable

    _INDEX_QUERY = """
        CREATE INDEX IF NOT EXISTS {0}
        ON "{1}"."{2}" ({3})
    """

    upload_call_count = 0

    _metadata = {}  # to be updated by operator, if applicable

    def __init__(
        self,
        source_conn_id,
        dwh_engine,
        dwh_conn_id,
        extract_strategy,
        load_strategy,
        target_table_name,
        target_schema_name,
        target_schema_suffix="_next",
        target_database_name=None,  # Only for Snowflake
        load_data_from=None,  # set a minimum date e.g. for reloading of data
        reload_data_from=None,  # load data from this date for new tables
        load_data_from_relative=None,  # optional timedelta for incremental
        load_data_until=None,  # set a maximum date
        load_data_until_relative=None,  # optional timedelta for incremental
        load_data_chunking_timedelta=None,  # optional timedelta to chunk by
        columns_definition=None,
        update_on_columns=None,
        primary_key_column_name=None,
        clean_data_before_upload=True,
        exclude_columns=[],  # list of columns to exclude, if no
        # columns_definition was supplied (e.g. for select * with sql)
        index_columns=[],  # list of columns to create an index on. can be
        # an expression, must be quoted in list if quoting is required.
        hash_columns=None,  # str or list of str - columns to hash pre-upload
        hashlib_func_name="sha256",  # specify hashlib hashing function
        wait_for_seconds=120,  # seconds past next_execution_date to wait until
        # wait_for_seconds only applies for incremental loads
        add_metadata=True,
        rename_columns: Optional[Dict[str, str]] = None,  # Rename columns
        subsequent_field=None,  # field name to use for subsequent extract strategy
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        assert not (rename_columns and columns_definition)
        assert rename_columns is None or isinstance(rename_columns, dict)

        # Check if the extract and load strategies are allowed in combination
        # Also check if required params are supplied for the strategies
        if extract_strategy == EC.ES_SUBSEQUENT:
            assert subsequent_field
            assert load_strategy in (
                EC.LS_UPSERT,
                EC.LS_INSERT_ADD,
                EC.LS_INSERT_REPLACE,
            )
            # insert_delete makes no sense in the subsequent context
        elif extract_strategy == EC.ES_FULL_REFRESH:
            assert load_strategy in (EC.LS_INSERT_REPLACE, EC.LS_INSERT_ADD)
            # upsert makes no sense - it's a full refresh!
            # insert_delete makes no sense - what to delete? Well, everyting!
        elif extract_strategy == EC.ES_INCREMENTAL:
            assert load_strategy in (EC.LS_UPSERT, EC.LS_INSERT_ADD)
            # replace makes no sense - it's incremental loading after all!
            # insert_delete makes sense but is not yet implemented
        else:
            raise Exception("Invalid extract_strategy {0}!".format(extract_strategy))

        if load_strategy == EC.LS_UPSERT:
            # upserts require a (composite) primary key of some sort
            if not (
                update_on_columns
                or primary_key_column_name
                or (
                    columns_definition
                    and (
                        0
                        < sum(
                            [
                                bool(columns_definition[col].get(EC.QBC_FIELD_PK))
                                for col in list(columns_definition.keys())
                            ]
                        )
                    )
                )
            ):
                raise Exception(
                    "If the load strategy is upsert, "
                    "one of the following is required:"
                    "\n- List of columns to update on (update_on_columns)"
                    "\n- Name of the primary key (primary_key_column_name)"
                    "\n- Columns definition (columns_definition) that includes"
                    " the primary key(s)"
                )
        elif load_strategy in (EC.LS_INSERT_ADD, EC.LS_INSERT_REPLACE):
            pass  # No requirements
        else:
            raise Exception("Invalid load_strategy {0}!".format(load_strategy))

        for item in EWAHBaseOperator.template_fields:
            # Make sure template_fields was not overwritten
            _msg = "Operator must not overwrite template_fields!"
            assert item in self.template_fields, _msg

        _msg = 'param "wait_for_seconds" must be a nonnegative integer!'
        assert isinstance(wait_for_seconds, int) and wait_for_seconds >= 0, _msg
        _msg = "extract_strategy {0} not accepted for this operator!".format(
            extract_strategy,
        )
        assert self._ACCEPTED_EXTRACT_STRATEGIES.get(extract_strategy), _msg

        if hash_columns and not clean_data_before_upload:
            _msg = "column hashing is only possible with data cleaning!"
            raise Exception(_msg)
        elif isinstance(hash_columns, str):
            hash_columns = [hash_columns]
        if hashlib_func_name:
            _msg = "Invalid hashing function: hashlib.{0}()"
            _msg = _msg.format(hashlib_func_name)
            assert hasattr(hashlib, hashlib_func_name), _msg

        if columns_definition and exclude_columns:
            raise Exception(
                "Must not supply both columns_definition and " + "exclude_columns!"
            )

        if not dwh_engine or not dwh_engine in EC.DWH_ENGINES:
            _msg = "Invalid DWH Engine: {0}\n\nAccepted Engines:\n\t{1}".format(
                str(dwh_engine),
                "\n\t".join(EC.DWH_ENGINES),
            )
            raise Exception(_msg)

        if index_columns and not dwh_engine == EC.DWH_ENGINE_POSTGRES:
            raise Exception("Indices are only allowed for PostgreSQL DWHs!")

        if (not dwh_engine == EC.DWH_ENGINE_SNOWFLAKE) and target_database_name:
            raise Exception('Received argument for "target_database_name"!')

        if self._REQUIRES_COLUMNS_DEFINITION:
            if not columns_definition:
                raise Exception(
                    "This operator requires the argument " + "columns_definition!"
                )

        if primary_key_column_name and update_on_columns:
            raise Exception(
                "Cannot supply BOTH primary_key_column_name AND" + " update_on_columns!"
            )

        _msg = "load_data_from_relative and load_data_until_relative must be"
        _msg += " timedelta if supplied!"
        assert isinstance(
            load_data_from_relative,
            (type(None), timedelta),
        ), _msg
        assert isinstance(
            load_data_until_relative,
            (type(None), timedelta),
        ), _msg
        _msg = "load_data_chunking_timedelta must be timedelta!"
        assert isinstance(
            load_data_chunking_timedelta,
            (type(None), timedelta),
        ), _msg

        self.source_conn_id = source_conn_id
        self.dwh_engine = dwh_engine
        self.dwh_conn_id = dwh_conn_id
        self.extract_strategy = extract_strategy
        self.load_strategy = load_strategy
        self.target_table_name = target_table_name
        self.target_schema_name = target_schema_name
        self.target_schema_suffix = target_schema_suffix
        self.target_database_name = target_database_name
        self.load_data_from = load_data_from
        self.reload_data_from = reload_data_from
        self.load_data_from_relative = load_data_from_relative
        self.load_data_until = load_data_until
        self.load_data_until_relative = load_data_until_relative
        self.load_data_chunking_timedelta = load_data_chunking_timedelta
        self.columns_definition = columns_definition
        if (not update_on_columns) and primary_key_column_name:
            if type(primary_key_column_name) == str:
                update_on_columns = [primary_key_column_name]
            elif type(primary_key_column_name) in (list, tuple):
                update_on_columns = primary_key_column_name
        self.update_on_columns = update_on_columns
        self.clean_data_before_upload = clean_data_before_upload
        self.primary_key_column_name = primary_key_column_name  # may be used ...
        #   ... by a child class at execution!
        self.exclude_columns = exclude_columns
        self.index_columns = index_columns
        self.hash_columns = hash_columns
        self.hashlib_func_name = hashlib_func_name
        self.wait_for_seconds = wait_for_seconds
        self.add_metadata = add_metadata
        self.rename_columns = rename_columns
        self.subsequent_field = subsequent_field

        self.uploader = get_uploader(self.dwh_engine)

        _msg = "DWH hook does not support extract strategy {0}!".format(
            extract_strategy,
        )
        # assert self.uploader._ACCEPTED_EXTRACT_STRATEGIES.get(extract_strategy), _msg

    def ewah_execute(self, context):
        raise Exception("You need to overwrite me!")

    def execute(self, context):
        """Why this method is defined here:
        When executing a task, airflow calls this method. Generally, this
        method contains the "business logic" of the individual operator.
        However, EWAH may want to do some actions for all operators. Thus,
        the child operators shall have an ewah_execute() function which is
        called by this general execute() method.
        """

        self.log.info(
            """

        Running EWAH Operator {0}.
        DWH: {1} (connection id: {2})
        Extract Strategy: {3}
        Load Strategy: {4}

        """.format(
                str(self),
                self.dwh_engine,
                self.dwh_conn_id,
                self.extract_strategy,
                self.load_strategy,
            )
        )

        # required for metadata in data upload
        self._execution_time = datetime_utcnow_with_tz()
        self._context = context

        self.uploader = self.uploader(EWAHBaseHook.get_connection(self.dwh_conn_id))

        if self.source_conn_id:
            # resolve conn id here & delete the object to avoid usage elsewhere
            self.source_conn = EWAHBaseHook.get_connection(self.source_conn_id)
            self.source_hook = self.source_conn.get_hook()
        del self.source_conn_id

        if self._CONN_TYPE:
            _msg = "Error - connection type must be {0}!".format(self._CONN_TYPE)
            assert self._CONN_TYPE == self.source_conn.conn_type, _msg

        temp_schema_name = self.target_schema_name + self.target_schema_suffix
        # Create a new copy of the target table.
        # This is so data is loaded into a new table and if data loading
        # fails, the original data is not corrupted. At a new try or re-run,
        # the original table is just copied anew.
        if not self.load_strategy == EC.LS_INSERT_REPLACE:
            # insert_replace always drops and replaces the tables completely
            self.uploader.copy_table(
                old_schema=self.target_schema_name,
                old_table=self.target_table_name,
                new_schema=temp_schema_name,
                new_table=self.target_table_name,
                database_name=self.target_database_name,
            )

        # set load_data_from and load_data_until as required
        data_from = ada(self.load_data_from)
        data_until = ada(self.load_data_until)
        if self.extract_strategy == EC.ES_INCREMENTAL:
            _tdz = timedelta(days=0)  # aka timedelta zero
            _ed = context["execution_date"]
            _ned = context["next_execution_date"]

            # normal incremental load
            _ed -= self.load_data_from_relative or _tdz
            data_from = max(_ed, data_from or _ed)
            if not self.test_if_target_table_exists():
                # Load data from scratch!
                data_from = ada(self.reload_data_from) or data_from

            _ned += self.load_data_until_relative or _tdz
            data_until = min(_ned, data_until or _ned)

        elif self.extract_strategy in (EC.ES_FULL_REFRESH, EC.ES_SUBSEQUENT):
            # Values may still be set as static values
            data_from = ada(self.reload_data_from) or data_from

        else:
            _msg = "Must define load_data_from etc. behavior for load strategy!"
            raise Exception(_msg)

        self.data_from = data_from
        self.data_until = data_until
        # del variables to make sure they are not used later on
        del self.load_data_from
        del self.reload_data_from
        del self.load_data_until
        del self.load_data_from_relative
        del self.load_data_until_relative

        # Have an option to wait until a short period (e.g. 2 minutes) past
        # the incremental loading range timeframe to ensure that all data is
        # loaded, useful e.g. if APIs lag or if server timestamps are not
        # perfectly accurate.
        # When a DAG is executed as soon as possible, some data sources
        # may not immediately have up to date data from their API.
        # E.g. querying all data until 12.30pm only gives all relevant data
        # after 12.32pm due to some internal delays. In those cases, make
        # sure the (incremental loading) DAGs don't execute too quickly.
        if self.wait_for_seconds and self.extract_strategy == EC.ES_INCREMENTAL:
            wait_until = context.get("next_execution_date")
            if wait_until:
                wait_until += timedelta(seconds=self.wait_for_seconds)
                self.log.info(
                    "Awaiting execution until {0}...".format(
                        str(wait_until),
                    )
                )
            while wait_until and datetime_utcnow_with_tz() < wait_until:
                # Only sleep a maximum of 5s at a time
                wait_for_timedelta = wait_until - datetime_utcnow_with_tz()
                time.sleep(max(0, min(wait_for_timedelta.total_seconds(), 5)))

        # execute operator
        if self.load_data_chunking_timedelta and data_from and data_until:
            # Chunking to avoid OOM
            assert data_until > data_from
            assert self.load_data_chunking_timedelta > timedelta(days=0)
            while self.data_from < data_until:
                self.data_until = self.data_from
                self.data_until += self.load_data_chunking_timedelta
                self.data_until = min(self.data_until, data_until)
                self.ewah_execute(context)
                self.data_from += self.load_data_chunking_timedelta
        else:
            self.ewah_execute(context)

        # if PostgreSQL and arg given: create indices
        for column in self.index_columns:
            assert self.dwh_engine == EC.DWH_ENGINE_POSTGRES
            # Use hashlib to create a unique 63 character string as index
            # name to avoid breaching index name length limits & accidental
            # duplicates / missing indices due to name truncation leading to
            # identical index names.
            self.uploader.dwh_hook.execute(
                self._INDEX_QUERY.format(
                    "__ewah_"
                    + hashlib.blake2b(
                        (
                            temp_schema_name
                            + "."
                            + self.target_table_name
                            + "."
                            + column
                        ).encode(),
                        digest_size=28,
                    ).hexdigest(),
                    self.target_schema_name + self.target_schema_suffix,
                    self.target_table_name,
                    column,
                )
            )

        # commit only at the end, so that no data may be committed before an
        # error occurs.
        self.log.info("Now committing changes!")
        self.uploader.commit()
        self.uploader.close()

    def test_if_target_table_exists(self):
        # Need to use existing hook to work within open transaction
        kwargs = {
            "table_name": self.target_table_name,
            "schema_name": self.target_schema_name + self.target_schema_suffix,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            kwargs["database_name"] = self.target_database_name
            return self.uploader.test_if_table_exists(**kwargs)
        if self.dwh_engine in [EC.DWH_ENGINE_POSTGRES, EC.DWH_ENGINE_SNOWFLAKE]:
            return self.uploader.test_if_table_exists(**kwargs)
        # For a new DWH, need to manually check if function works properly
        # Thus, fail until explicitly added
        raise Exception("Function not implemented!")

    def get_max_value_of_column(self, column_name):
        # Need to use existing hook to work within open transaction
        kwargs = {
            "column_name": column_name,
            "table_name": self.target_table_name,
            "schema_name": self.target_schema_name + self.target_schema_suffix,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            kwargs["database_name"] = self.target_database_name
            return self.uploader.get_max_value_of_column(**kwargs)
        if self.dwh_engine in [EC.DWH_ENGINE_POSTGRES, EC.DWH_ENGINE_SNOWFLAKE]:
            return self.uploader.get_max_value_of_column(**kwargs)
        # For a new DWH, need to manually check if function works properly
        # Thus, fail until explicitly added
        raise Exception("Function not implemented!")

    def _create_columns_definition(self, data):
        "Create a columns_definition from data (list of dicts)."
        inconsistent_data_type = EC.QBC_TYPE_MAPPING[self.dwh_engine].get(
            EC.QBC_TYPE_MAPPING_INCONSISTENT
        )

        def get_field_type(value):
            return (
                EC.QBC_TYPE_MAPPING[self.dwh_engine].get(type(value))
                or inconsistent_data_type
            )

        result = {}
        for datum in data:
            for field in datum.keys():
                if field in self.exclude_columns:
                    datum[field] = None
                elif field in (self.hash_columns or []) and not result.get(field):
                    # Type is appropriate string type & QBC_FIELD_HASH is true
                    result.update(
                        {
                            field: {
                                EC.QBC_FIELD_TYPE: get_field_type("str"),
                                EC.QBC_FIELD_HASH: True,
                            }
                        }
                    )
                elif not (
                    result.get(field, {}).get(EC.QBC_FIELD_TYPE)
                    == inconsistent_data_type
                ) and (not datum[field] is None):
                    if result.get(field):
                        # column has been added in a previous iteration.
                        # If not default column: check if new and old column
                        #   type identification agree.
                        if not (
                            result[field][EC.QBC_FIELD_TYPE]
                            == get_field_type(datum[field])
                        ):
                            self.log.info(
                                "WARNING! Data types are inconsistent."
                                + " Affected column: {0}".format(field)
                            )
                            result[field][EC.QBC_FIELD_TYPE] = inconsistent_data_type

                    else:
                        # First iteration with this column. Add to result.
                        result.update(
                            {field: {EC.QBC_FIELD_TYPE: get_field_type(datum[field])}}
                        )
        return result

    def upload_data(self, data=None, columns_definition=None):
        """Upload data, no matter the source. Call this functions in the child
        operator whenever data is available for upload, as often as needed.
        """
        if not data:
            self.log.info("No data to upload!")
            return
        self.upload_call_count += 1
        self.log.info(
            "Chunk {1}: Uploading {0} rows of data.".format(
                str(len(data)),
                str(self.upload_call_count),
            )
        )

        if self.add_metadata or self.rename_columns:
            if self.add_metadata:
                self.log.info("Adding metadata...")
                metadata = copy.deepcopy(self._metadata)  # from individual operator
                # for all operators alike
                metadata.update(
                    {
                        "_ewah_extract_strategy": self.extract_strategy,
                        "_ewah_load_strategy": self.load_strategy,
                        "_ewah_executed_at": self._execution_time,
                        "_ewah_execution_chunk": self.upload_call_count,
                        "_ewah_dag_id": self._context["dag"].dag_id,
                        "_ewah_dag_run_id": self._context["run_id"],
                        "_ewah_dag_run_execution_date": self._context["execution_date"],
                        "_ewah_dag_run_next_execution_date": self._context[
                            "next_execution_date"
                        ],
                    }
                )
            rename_columns = self.rename_columns or {}
            for datum in data:
                if self.add_metadata:
                    datum.update(metadata)
                if rename_columns:
                    for (old_name, new_name) in rename_columns.items():
                        datum[new_name] = datum.pop(old_name, None)

        columns_definition = columns_definition or self.columns_definition
        if not columns_definition:
            self.log.info("Creating table schema on the fly based on data.")
            columns_definition = self._create_columns_definition(data)

        if self.update_on_columns:
            pk_list = self.update_on_columns  # is a list already
        elif self.primary_key_column_name:
            pk_list = [self.primary_key_column_name]
        else:
            pk_list = []

        if pk_list:
            for pk_name in pk_list:
                if not pk_name in columns_definition.keys():
                    raise Exception(
                        ("Column {0} does not exist but is " + "expected!").format(
                            pk_name
                        )
                    )
                columns_definition[pk_name][EC.QBC_FIELD_PK] = True

        if (self.upload_call_count > 1) or (
            not (self.load_strategy == EC.LS_INSERT_REPLACE)
        ):
            self.log.info("Checking for, and applying schema changes.")
            _new_schema_name = self.target_schema_name + self.target_schema_suffix
            new_cols, del_cols = self.uploader.detect_and_apply_schema_changes(
                new_schema_name=_new_schema_name,
                new_table_name=self.target_table_name,
                new_columns_dictionary=columns_definition,
                # When introducing a feature utilizing this, remember to
                #  consider multiple runs within the same execution
                drop_missing_columns=False and self.upload_call_count == 1,
                database=self.target_database_name,
                commit=False,  # Commit only when / after uploading data
            )
            self.log.info(
                "Added fields:\n\t{0}\nDeleted fields:\n\t{1}".format(
                    "\n\t".join(new_cols) or "\n",
                    "\n\t".join(del_cols) or "\n",
                )
            )

        self.log.info("Uploading data now.")
        self.uploader.create_or_update_table(
            data=data,
            load_strategy=self.load_strategy,
            upload_call_count=self.upload_call_count,
            columns_definition=columns_definition,
            table_name=self.target_table_name,
            schema_name=self.target_schema_name,
            schema_suffix=self.target_schema_suffix,
            database_name=self.target_database_name,
            update_on_columns=self.update_on_columns,
            commit=False,  # See note below for reason
            clean_data_before_upload=self.clean_data_before_upload,
            hash_columns=self.hash_columns,
            hashlib_func_name=self.hashlib_func_name,
        )
        """ Note on committing changes:
            The hook used for data uploading is created at the beginning of the
            execute function and automatically committed and closed at the end.
            DO NOT commit in this function, as multiple uploads may be required,
            and any intermediate commit may be subsequently followed by an
            error, which would then result in incomplete data committed.
        """
