try: # load operators with dependencies installed, ignore others
    from ewah.operators.fx_operator import EWAHFXOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHFXOperator
try:
    from ewah.operators.google_analytics_operator import EWAHGAOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHGAOperator
try:
    from ewah.operators.mysql_operator import EWAHMySQLOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHMySQLOperator
try:
    from ewah.operators.oracle_operator import EWAHOracleSQLOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHOracleSQLOperator
try:
    from ewah.operators.postgres_operator import EWAHPostgresOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHPostgresOperator
try:
    from ewah.operators.s3_operator import EWAHS3Operator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHS3Operator
try:
    from ewah.operators.facebook_operator import EWAHFBOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHFBOperator
try:
    from ewah.operators.google_sheets_operator import EWAHGSpreadOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHGSpreadOperator
try:
    from ewah.operators.mongodb_operator import EWAHMongoDBOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHGSpreadOperator
try:
    from ewah.operators.shopify_operator import EWAHShopifyOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHShopifyOperator
try:
    from ewah.operators.zendesk_operator import EWAHZendeskOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHZendeskOperator
try:
    from ewah.operators.google_ads_operator import EWAHGoogleAdsOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHGoogleAdsOperator
try:
    from ewah.operators.google_maps_operator import EWAHGMapsOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHGMapsOperator
try:
    from ewah.operators.mailchimp_operator import EWAHMailchimpOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHMailchimpOperator
try:
    from ewah.operators.bigquery_operator import EWAHBigQueryOperator
except ImportError:
    from ewah.operators.base_operator import EWAHEmptyOperator as EWAHBigQueryOperator
