from datetime import datetime, date, timedelta
from uuid import UUID


class EWAHConstants:
    "This class contains a number of constants for use throughout Ewah."

    # Available DWH Engines - use all small caps!
    DWH_ENGINE_POSTGRES = "postgresql"
    DWH_ENGINE_SNOWFLAKE = "snowflake"
    DWH_ENGINE_BIGQUERY = "bigquery"
    DWH_ENGINE_REDSHIFT = "redshift"
    DWH_ENGINE_S3 = "s3"
    DWH_ENGINE_GS = "googlesheets"
    DWH_ENGINES = [
        DWH_ENGINE_POSTGRES,
        DWH_ENGINE_SNOWFLAKE,
        DWH_ENGINE_BIGQUERY,
        # DWH_ENGINE_REDSHIFT, To Be Implemented
        # DWH_ENGINE_S3, To Be Implemented
        DWH_ENGINE_GS,
    ]

    DWH_ENGINE_SPELLING_VARIANTS = {
        "bq": DWH_ENGINE_BIGQUERY,
        "postgres": DWH_ENGINE_POSTGRES,
        "snowflake": DWH_ENGINE_SNOWFLAKE,
        "sheets": DWH_ENGINE_GS,
        "gsheets": DWH_ENGINE_GS,
        "google sheets": DWH_ENGINE_GS,
        DWH_ENGINE_POSTGRES: DWH_ENGINE_POSTGRES,
        DWH_ENGINE_SNOWFLAKE: DWH_ENGINE_SNOWFLAKE,
        DWH_ENGINE_GS: DWH_ENGINE_GS,
        DWH_ENGINE_BIGQUERY: DWH_ENGINE_BIGQUERY,
    }

    # DAG Strategies
    DS_ATOMIC = "atomic"
    DS_MIXED = "mixed"
    DS_IDEMPOTENT = "idempotent"

    # Available Extract Strategies
    ES_FULL_REFRESH = "full-refresh"  # load all available data
    ES_INCREMENTAL = "incremental"  # just load data pertaining to a certain time
    ES_SUBSEQUENT = "subsequent"  # just load newest data

    # Available Load Strategies
    LS_UPSERT = "upsert"  # update data based on a (composite) primary key
    LS_INSERT_ADD = "insert_add"  # just append data
    LS_INSERT_REPLACE = "insert_replace"  # replace data
    LS_INSERT_DELETE = "insert_delete"  # delete relevant data before writing new data

    # Default load strategy depending on extract strategy
    DEFAULT_LS_PER_ES = {
        ES_FULL_REFRESH: LS_INSERT_REPLACE,
        ES_SUBSEQUENT: LS_UPSERT,
        ES_INCREMENTAL: LS_UPSERT,
    }

    # EC.LS_FULLCREMENTAL = 'fullcremental'
    # fullcremental is a mix of full refresh and incremental
    # --> not an independent load strategy!

    # Query Building Constants
    QBC_FIELD_TYPE = "data_type"

    """If a field is of inconsistent data type or type does not exist
    in the mapping below, use inconsistent value specified here."""
    QBC_TYPE_MAPPING = {
        DWH_ENGINE_POSTGRES: {
            str: "text",
            int: "bigint",
            float: "numeric",
            dict: "jsonb",
            list: "jsonb",
            tuple: "jsonb",
            set: "jsonb",
            frozenset: "jsonb",
            bool: "boolean",
            datetime: "timestamp with time zone",
            date: "date",
            timedelta: "interval",
            UUID: "text",
        },
        DWH_ENGINE_SNOWFLAKE: {
            str: "TEXT",
            int: "FLOAT",  # Temporary fix - need to implement dynamic data type changes
            float: "FLOAT",
            dict: "OBJECT",
            list: "ARRAY",
            tuple: "ARRAY",
            set: "ARRAY",
            frozenset: "ARRAY",
            bool: "BOOLEAN",
            datetime: "TIMESTAMP_TZ",
            date: "DATE",
            UUID: "TEXT",
        },
        DWH_ENGINE_BIGQUERY: {
            str: "STRING",
            int: "INT64",
            float: "FLOAT64",
            # Cannot use struct as data type because if there is any difference
            # in the structure of the mapping types, BigQuery loading will fail
            dict: "STRING",  # "STRUCT",
            list: "STRING",  # "STRUCT",
            tuple: "STRING",  # "STRUCT",
            set: "STRING",  # "STRUCT",
            frozenset: "STRING",  # "STRUCT",
            bool: "BOOL",
            datetime: "TIMESTAMP",
            date: "DATE",
            bytes: "BYTES",
            UUID: "STRING",
        },
        # DWH_ENGINE_REDSHIFT: {},
        # DWH_ENGINE_S3: {},
        DWH_ENGINE_GS: {  # must have these two values evaluate to True as bool
            # EWAH tries to dump all non-mapped types, thus map even if map is never used!
            str: "x",
            int: "x",
            float: "x",
            dict: "x",
            list: "x",
            tuple: "x",
            set: "x",
            frozenset: "x",
            bool: "x",
            datetime: "timestamp with time x",
            date: "x",
            UUID: "x",
        },
    }
