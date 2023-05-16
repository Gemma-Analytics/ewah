from airflow.models import BaseOperator

from ewah.cleaner import EWAHCleaner
from ewah.constants import EWAHConstants as EC
from ewah.utils.airflow_utils import (
    datetime_utcnow_with_tz,
    airflow_datetime_adjustments as ada,
)
from ewah.hooks.base import EWAHBaseHook
from ewah.uploaders import get_uploader

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Union

import copy
import hashlib
import sys
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

    _CONN_TYPE = None  # overwrite me with the required connection type, if applicable

    _INDEX_QUERY = """
        CREATE INDEX IF NOT EXISTS {0}
        ON "{1}"."{2}" ({3})
    """

    _metadata = {}  # to be updated by operator, if applicable

    def __init__(self, *args, **kwargs):
        try:
            dag = kwargs.get("dag")  # For error handling below
            return self.base_init(*args, **kwargs)
        except Exception as e:
            # Add useful information to the raised Exception as it will be displayed
            # in the Airflow UI
            exc_type, exc_obj, exc_tb = sys.exc_info()
            try:
                dag_id = (dag or self.dag).dag_id
            except Exception as ex:
                dag_id = "[unknown]"
            raise Exception(
                """

            EWAH Failed to import the DAG.

            \tDAG: {3}
            \tTask: {4}
            \tOperator: {2}

            \tException Type: {0}
            \tError: {1}

            See below for positional and keyword: {5}

            """.replace(
                    "            ", ""
                ).format(
                    exc_type,
                    str(e),
                    type(self).__name__,
                    dag_id,
                    kwargs["task_id"],
                    "\n\n\tPositional arguments:\n\t\t"
                    + "\n\t\t".join(args or ["(None)"])
                    + "\n\n\tKeyword arguments:\n\t\t"
                    + "\n\t\t".join(
                        ["{0}: {1}".format(k, str(v)) for (k, v) in kwargs.items()]
                        or ["(None)"]
                    ),
                )
            )

    def base_init(
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
        primary_key=None,  # either string or list of strings
        include_columns=None,  # list of columns to explicitly include
        exclude_columns=None,  # list of columns to exclude
        index_columns=[],  # list of columns to create an index on. can be
        # an expression, must be quoted in list if quoting is required.
        hash_columns=None,  # str or list of str - columns to hash pre-upload
        hash_salt=None,  # string salt part for hashing
        wait_for_seconds=120,  # seconds past data_interval_end to wait until
        # wait_for_seconds only applies for incremental loads
        add_metadata=True,
        rename_columns: Optional[Dict[str, str]] = None,  # Rename columns
        subsequent_field=None,  # field name to use for subsequent extract strategy
        default_timezone=None,  # specify a default time zone for tz-naive datetimes
        use_temp_pickling=True,  # use new upload method if True - use it by default
        pickling_upload_chunk_size=100000,  # default chunk size for pickle upload
        pickle_compression=None,  # data compression algorithm to use for pickles
        default_values=None,  # dict with default values for columns (to avoid nulls)
        cleaner_class=EWAHCleaner,
        cleaner_callables=None,  # callables or list of callables to run during cleaning
        uploader_class=None,  # Future: deprecate dwh_engine and use this kwarg instead
        additional_uploader_kwargs=None,
        deduplication_before_upload=False,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        if default_values:
            assert isinstance(default_values, dict)

        assert pickle_compression is None or pickle_compression in (
            "gzip",
            "bz2",
            "lzma",
        )

        assert isinstance(rename_columns, (type(None), dict))

        if default_timezone:
            assert dwh_engine in (EC.DWH_ENGINE_POSTGRES,)  # Only for PostgreSQL so far
            assert not ";" in default_timezone  # Avoid SQL Injection

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
            if not primary_key:
                raise Exception(
                    "If the load strategy is upsert, name of the primary"
                    " key(s) (primary_key) is required!"
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

        if isinstance(primary_key, str):
            primary_key = [primary_key]

        if isinstance(hash_columns, str):
            hash_columns = [hash_columns]

        if isinstance(include_columns, str):
            include_columns = [include_columns]

        if include_columns and primary_key:
            for col in primary_key:
                if not col in include_columns:
                    _msg = """
                        Primary key {0} is not in the include_columns list.
                        Make sure all primary keys are included.
                        """.format(
                        col
                    )
                    raise Exception(_msg)

        if exclude_columns and isinstance(exclude_columns, str):
            exclude_columns = [exclude_columns]

        if include_columns and exclude_columns:
            _msg = "Don't use include and exclude columns config at the same time!"
            raise Exception(_msg)

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

        if callable(cleaner_callables):
            cleaner_callables = [cleaner_callables]

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
        self.primary_key = primary_key  # may be used ...
        #   ... by a child class at execution!
        self.include_columns = include_columns
        self.exclude_columns = exclude_columns
        self.index_columns = index_columns
        self.hash_columns = hash_columns
        self.hash_salt = hash_salt
        self.wait_for_seconds = wait_for_seconds
        self.add_metadata = add_metadata
        self.rename_columns = rename_columns
        self.subsequent_field = subsequent_field
        self.default_timezone = default_timezone
        self.use_temp_pickling = use_temp_pickling
        self.pickling_upload_chunk_size = pickling_upload_chunk_size
        self.pickle_compression = pickle_compression
        self.default_values = default_values
        self.cleaner_class = cleaner_class
        self.cleaner_callables = cleaner_callables
        self.deduplication_before_upload = deduplication_before_upload

        self.uploader_class = uploader_class or get_uploader(self.dwh_engine)
        self.additional_uploader_kwargs = additional_uploader_kwargs or {}

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

        cleaner_callables = self.cleaner_callables or []

        if self.source_conn_id:
            # resolve conn id here & delete the object to avoid usage elsewhere
            self.source_conn = EWAHBaseHook.get_connection(self.source_conn_id)
            self.source_hook = self.source_conn.get_hook()
            if callable(getattr(self.source_hook, "get_cleaner_callables", None)):
                hook_callables = self.source_hook.get_cleaner_callables()
                if callable(hook_callables):
                    cleaner_callables.append(hook_callables)
                elif hook_callables:
                    # Ought to be list of callables
                    cleaner_callables += hook_callables
        del self.source_conn_id

        if self._CONN_TYPE:
            assert (
                self._CONN_TYPE == self.source_conn.conn_type
            ), "Error - connection type must be {0}!".format(self._CONN_TYPE)

        uploader_callables = self.uploader_class.get_cleaner_callables()
        if callable(uploader_callables):
            cleaner_callables.append(uploader_callables)
        elif uploader_callables:
            cleaner_callables += uploader_callables

        self.uploader = self.uploader_class(
            dwh_conn=EWAHBaseHook.get_connection(self.dwh_conn_id),
            cleaner=self.cleaner_class(
                default_row=self.default_values,
                include_columns=self.include_columns,
                exclude_columns=self.exclude_columns,
                add_metadata=self.add_metadata,
                rename_columns=self.rename_columns,
                hash_columns=self.hash_columns,
                hash_salt=self.hash_salt,
                additional_callables=cleaner_callables,
            ),
            table_name=self.target_table_name,
            schema_name=self.target_schema_name,
            schema_suffix=self.target_schema_suffix,
            database_name=self.target_database_name,
            primary_key=self.primary_key,
            load_strategy=self.load_strategy,
            use_temp_pickling=self.use_temp_pickling,
            pickling_upload_chunk_size=self.pickling_upload_chunk_size,
            pickle_compression=self.pickle_compression,
            deduplication_before_upload=self.deduplication_before_upload,
            **self.additional_uploader_kwargs,
        )

        try:
            # If applicable: set the session's default time zone
            if self.default_timezone:
                self.uploader.dwh_hook.execute(
                    "SET timezone TO '{0}'".format(self.default_timezone)
                )

            # Create a new copy of the target table.
            # This is so data is loaded into a new table and if data loading
            # fails, the original data is not corrupted. At a new try or re-run,
            # the original table is just copied anew.
            if not self.load_strategy == EC.LS_INSERT_REPLACE:
                # insert_replace always drops and replaces the tables completely
                self.uploader.copy_table()

            # set load_data_from and load_data_until as required
            data_from = ada(self.load_data_from)
            data_until = ada(self.load_data_until)
            if self.extract_strategy == EC.ES_INCREMENTAL:
                _tdz = timedelta(days=0)  # aka timedelta zero
                _ed = context["data_interval_start"]
                _ned = context["data_interval_end"]

                # normal incremental load
                _ed -= self.load_data_from_relative or _tdz
                data_from = min(_ed, data_from or _ed)
                if not self.test_if_target_table_exists():
                    # Load data from scratch!
                    data_from = ada(self.reload_data_from) or data_from

                _ned += self.load_data_until_relative or _tdz
                data_until = max(_ned, data_until or _ned)

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
            del self.load_data_until_relative
            if not self.extract_strategy == EC.ES_SUBSEQUENT:
                # keep this param for subsequent loads
                del self.load_data_from_relative

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
                wait_until = context.get("data_interval_end")
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
                    self.data_until = min(
                        self.data_from + self.load_data_chunking_timedelta, data_until
                    )
                    self.log.info(
                        "Now loading from {0} to {1}...".format(
                            str(self.data_from), str(self.data_until)
                        )
                    )
                    self.ewah_execute(context)
                    self.data_from += self.load_data_chunking_timedelta
            else:
                self.ewah_execute(context)

            # Run final scripts
            # TODO: Include indexes into uploader and then remove this step
            self.uploader.finalize_upload()

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
                                self.target_schema_name
                                + self.target_schema_suffix
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
        finally:
            self.uploader.close()
            del self.uploader

    def test_if_target_table_exists(self):
        # TODO: move this function to uploader
        # Need to use existing hook to work within open transaction
        kwargs = {
            "table_name": self.target_table_name,
            "schema_name": self.target_schema_name + self.target_schema_suffix,
        }
        if self.dwh_engine == EC.DWH_ENGINE_SNOWFLAKE:
            kwargs["database_name"] = self.target_database_name
            return self.uploader.test_if_table_exists(**kwargs)
        if self.dwh_engine == EC.DWH_ENGINE_BIGQUERY:
            kwargs["project_id"] = self.target_database_name
            return self.uploader.test_if_table_exists(**kwargs)
        if self.dwh_engine == EC.DWH_ENGINE_POSTGRES:
            return self.uploader.test_if_table_exists(**kwargs)
        # For a new DWH, need to manually check if function works properly
        # Thus, fail until explicitly added
        raise Exception("Function not implemented!")

    def get_max_value_of_column(self, column_name):
        # Deprecated - better to call uploader directly if able
        return self.uploader.get_max_value_of_column(column_name=column_name)

    def upload_data(self, data=None):
        """Upload data, no matter the source. Call this functions in the child
        operator whenever data is available for upload, as often as needed.
        """
        if not data:
            self.log.info("No data to upload!")
            return

        if self.add_metadata:
            metadata = copy.deepcopy(self._metadata)  # from individual operator
            interval_start = self._context["data_interval_start"]
            interval_start = datetime.fromtimestamp(
                interval_start.timestamp(), interval_start.tz
            )
            interval_end = self._context["data_interval_end"]
            interval_end = datetime.fromtimestamp(
                interval_end.timestamp(), interval_end.tz
            )
            metadata.update(
                {
                    "_ewah_extract_strategy": self.extract_strategy,
                    "_ewah_load_strategy": self.load_strategy,
                    "_ewah_executed_at": self._execution_time,
                    "_ewah_dag_id": self._context["dag"].dag_id,
                    "_ewah_dag_run_id": self._context["run_id"],
                    "_ewah_dag_run_data_interval_start": interval_start,
                    "_ewah_dag_run_data_interval_end": interval_end,
                }
            )
        else:
            metadata = None

        return self.uploader.upload_data(data, metadata)
