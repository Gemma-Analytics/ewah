from datetime import datetime

class EWAHConstants:
    "This class contains a number of constants for use throughout Ewah."

    # Available DWH Engines
    DWH_ENGINE_POSTGRES = 'PostgreSQL'
    DWH_ENGINE_SNOWFLAKE = 'Snowflake'
    DWH_ENGINE_BIGQUERY = 'BigQuery'
    DWH_ENGINE_REDSHIFT = 'Redshift'
    DWH_ENGINE_S3 = 'S3'
    DWH_ENGINES = [
        DWH_ENGINE_POSTGRES,
        DWH_ENGINE_SNOWFLAKE,
        # DWH_ENGINE_BIGQUERY, To Be Implemented
        # DWH_ENGINE_REDSHIFT, To Be Implemented
        # DWH_ENGINE_S3, To Be Implemented
    ]

    # Query Building Constants
    QBC_FIELD_TYPE = 'ft'

    QBC_FIELD_PK = 'pk'
    QBC_FIELD_NN = 'nn'
    QBC_FIELD_UQ = 'uq'
    QBC_FIELD_GSHEET_COLNO = 'column' # Gsheet operator: position of the column
    QBC_FIELD_CONSTRAINTS_MAPPING = {
        DWH_ENGINE_POSTGRES: {
            QBC_FIELD_PK: 'PRIMARY KEY',
            QBC_FIELD_NN: 'NOT NULL',
            QBC_FIELD_UQ: 'UNIQUE',
        },
        DWH_ENGINE_SNOWFLAKE: {
            QBC_FIELD_PK: 'PRIMARY KEY',
            QBC_FIELD_NN: 'NOT NULL',
            QBC_FIELD_UQ: 'UNIQUE',
        },
        # DWH_ENGINE_BIGQUERY: {},
        # DWH_ENGINE_REDSHIFT: {},
        # DWH_ENGINE_S3: {},
    }

    """When a columns_definition is given, but a column does not have a field
    type defined, use default as specified here. If no columns_definition is
    given, and a field is of inconsistent data type or type does not exist
    in the mapping below, use inconsistent value specified here."""
    QBC_TYPE_MAPPING_DEFAULT = 'tm_default'
    QBC_TYPE_MAPPING_INCONSISTENT = 'tm_incon'
    QBC_TYPE_MAPPING = {
        DWH_ENGINE_POSTGRES: {
            QBC_TYPE_MAPPING_DEFAULT: 'text',
            QBC_TYPE_MAPPING_INCONSISTENT: 'text',
            type(''): 'text',
            type(1): 'bigint',
            type(1.2): 'numeric',
            type({}): 'jsonb',
            type([]): 'jsonb',
            type(True): 'boolean',
            type(datetime.now()): 'timestamp with time zone',
        },
        DWH_ENGINE_SNOWFLAKE: {
            QBC_TYPE_MAPPING_DEFAULT: 'text',
            QBC_TYPE_MAPPING_INCONSISTENT: 'text',
            type(''): 'text',
            type(1): 'bigint',
            type(1.2): 'numeric',
            type({}): 'jsonb',
            type([]): 'jsonb',
            type(True): 'boolean',
            type(datetime.now()): 'timestamp with time zone',
        },
        # DWH_ENGINE_BIGQUERY: {},
        # DWH_ENGINE_REDSHIFT: {},
        # DWH_ENGINE_S3: {},
    }
