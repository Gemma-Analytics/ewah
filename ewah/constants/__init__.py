from datetime import datetime, date, timedelta


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
        # DWH_ENGINE_BIGQUERY, To Be Implemented
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

    # EC.LS_FULLCREMENTAL = 'fullcremental'
    # fullcremental is a mix of full refresh and incremental
    # --> not an independent load strategy!

    # Query Building Constants
    QBC_FIELD_TYPE = "data_type"

    QBC_FIELD_PK = "is_primary_key"
    QBC_FIELD_HASH = "is_hash_column"
    QBC_FIELD_GSHEET_COLNO = "column"  # Gsheet operator: position of the column
    QBC_FIELD_CONSTRAINTS_MAPPING = {
        DWH_ENGINE_POSTGRES: {
            QBC_FIELD_PK: "PRIMARY KEY",
        },
        DWH_ENGINE_SNOWFLAKE: {
            QBC_FIELD_PK: "PRIMARY KEY",
        },
        # DWH_ENGINE_BIGQUERY: {},
        # DWH_ENGINE_REDSHIFT: {},
        # DWH_ENGINE_S3: {},
        DWH_ENGINE_GS: {
            # Not applicable
        },
    }

    """When a columns_definition is given, but a column does not have a field
    type defined, use default as specified here. If no columns_definition is
    given, and a field is of inconsistent data type or type does not exist
    in the mapping below, use inconsistent value specified here."""
    QBC_TYPE_MAPPING_DEFAULT = "tm_default"
    QBC_TYPE_MAPPING_INCONSISTENT = "tm_incon"
    QBC_TYPE_MAPPING = {
        DWH_ENGINE_POSTGRES: {
            QBC_TYPE_MAPPING_DEFAULT: "text",
            QBC_TYPE_MAPPING_INCONSISTENT: "text",
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
        },
        DWH_ENGINE_SNOWFLAKE: {
            QBC_TYPE_MAPPING_DEFAULT: "VARCHAR",
            QBC_TYPE_MAPPING_INCONSISTENT: "VARCHAR",
            str: "VARCHAR",
            int: "NUMBER",
            float: "NUMBER",
            dict: "OBJECT",
            list: "ARRAY",
            tuple: "ARRAY",
            set: "ARRAY",
            frozenset: "ARRAY",
            bool: "BOOLEAN",
            datetime: "TIMESTAMP_TZ",
            date: "DATE",
        },
        DWH_ENGINE_BIGQUERY: {
            QBC_TYPE_MAPPING_DEFAULT: "STRING",
            QBC_TYPE_MAPPING_INCONSISTENT: "STRING",
            str: "STRING",
            int: "INT64",
            float: "NUMERIC",
            dict: "STRUCT",
            list: "STRUCT",
            tuple: "STRUCT",
            set: "STRUCT",
            frozenset: "STRUCT",
            bool: "BOOL",
            datetime: "TIMESTAMP",
            date: "DATE",
            bytes: "BYTES",
        },
        # DWH_ENGINE_REDSHIFT: {},
        # DWH_ENGINE_S3: {},
        DWH_ENGINE_GS: {  # must have these two values evaluate to True as bool
            QBC_TYPE_MAPPING_DEFAULT: "x",
            QBC_TYPE_MAPPING_INCONSISTENT: "x",
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
        },
    }
