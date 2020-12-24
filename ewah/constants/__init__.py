from datetime import datetime, date

class EWAHConstants:
    "This class contains a number of constants for use throughout Ewah."

    # Available DWH Engines
    DWH_ENGINE_POSTGRES = 'PostgreSQL'
    DWH_ENGINE_SNOWFLAKE = 'Snowflake'
    DWH_ENGINE_BIGQUERY = 'BigQuery'
    DWH_ENGINE_REDSHIFT = 'Redshift'
    DWH_ENGINE_S3 = 'S3'
    DWH_ENGINE_GS = 'GoogleSheets'
    DWH_ENGINES = [
        DWH_ENGINE_POSTGRES,
        DWH_ENGINE_SNOWFLAKE,
        # DWH_ENGINE_BIGQUERY, To Be Implemented
        # DWH_ENGINE_REDSHIFT, To Be Implemented
        # DWH_ENGINE_S3, To Be Implemented
        DWH_ENGINE_GS,
    ]

    # Available Operators (smallcap only)
    OPERATORS_LIST = {
        'aircall': 'EWAHAircallOperator',
        'bigquery': 'EWAHBigQueryOperator',
        'bq': 'EWAHBigQueryOperator',
        'dynamodb': 'EWAHDynamoDBOperator',
        'facebook': 'EWAHFBOperator',
        'fb': 'EWAHFBOperator',
        'fx': 'EWAHFXOperator',
        'ga': 'EWAHGAOperator',
        'gads': 'EWAHGoogleAdsOperator',
        'gmaps': 'EWAHGMapsOperator',
        'google_ads': 'EWAHGoogleAdsOperator',
        'google_analytics': 'EWAHGAOperator',
        'google_sheets': 'EWAHGSpreadOperator',
        'googlemaps': 'EWAHGMapsOperator',
        'gs': 'EWAHGSpreadOperator',
        'gsheets': 'EWAHGSpreadOperator',
        'hubspot': 'EWAHHubspotOperator',
        'mailchimp': 'EWAHMailchimpOperator',
        'mailingwork': 'EWAHMailingworkOperator',
        'mc': 'EWAHMailchimpOperator',
        'mongo': 'EWAHMongoDBOperator',
        'mongodb': 'EWAHMongoDBOperator',
        'mysql': 'EWAHMySQLOperator',
        'oracle': 'EWAHOracleSQLOperator',
        'pgsql': 'EWAHPostgresOperator',
        'postgres': 'EWAHPostgresOperator',
        'postgresql': 'EWAHPostgresOperator',
        's3': 'EWAHS3Operator',
        'shopify': 'EWAHShopifyOperator',
        'stripe': 'EWAHStripeOperator',
        'zendesk': 'EWAHZendeskOperator',
    }

    # Available Load Strategies
    LS_FULL_REFRESH = 'full-refresh'
    LS_INCREMENTAL = 'incremental'
    # EC.LS_FULLCREMENTAL = 'fullcremental'
    # fullcremental is a mix of full refresh and incremental
    # --> not an independent load strategy!
    LS_APPENDING = 'appending'

    # Query Building Constants
    QBC_FIELD_TYPE = 'data_type'

    QBC_FIELD_PK = 'is_primary_key'
    QBC_FIELD_NN = 'is_not_null'
    QBC_FIELD_UQ = 'is_unique'
    QBC_FIELD_HASH = 'is_hash_column'
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
        DWH_ENGINE_GS: {
            # Not applicable
        },
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
            str: 'text',
            int: 'bigint',
            float: 'numeric',
            dict: 'jsonb',
            list: 'jsonb',
            tuple: 'jsonb',
            set: 'jsonb',
            frozenset: 'jsonb',
            bool: 'boolean',
            datetime: 'timestamp with time zone',
            date: 'date',
        },
        DWH_ENGINE_SNOWFLAKE: {
            QBC_TYPE_MAPPING_DEFAULT: 'VARIANT',
            QBC_TYPE_MAPPING_INCONSISTENT: 'VARIANT',
            str: 'VARCHAR',
            int: 'NUMBER',
            float: 'NUMBER',
            dict: 'OBJECT',
            list: 'ARRAY',
            tuple: 'ARRAY',
            set: 'ARRAY',
            frozenset: 'ARRAY',
            bool: 'BOOLEAN',
            datetime: 'TIMESTAMP_TZ',
            date: 'DATE',
        },
        DWH_ENGINE_BIGQUERY: {
            QBC_TYPE_MAPPING_DEFAULT: 'STRING',
            QBC_TYPE_MAPPING_INCONSISTENT: 'STRING',
            str: 'STRING',
            int: 'INT64',
            float: 'NUMERIC',
            dict: 'STRUCT',
            list: 'STRUCT',
            tuple: 'STRUCT',
            set: 'STRUCT',
            frozenset: 'STRUCT',
            bool: 'BOOL',
            datetime: 'TIMESTAMP',
            date: 'DATE',
            bytes: 'BYTES',
        },
        # DWH_ENGINE_REDSHIFT: {},
        # DWH_ENGINE_S3: {},
        DWH_ENGINE_GS: { # must have these two values evaluate to True as bool
            QBC_TYPE_MAPPING_DEFAULT: 'x',
            QBC_TYPE_MAPPING_INCONSISTENT: 'x',
        },
    }
