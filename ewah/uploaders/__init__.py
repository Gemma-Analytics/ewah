from ewah.constants import EWAHConstants as EC
from ewah.uploaders.snowflake import EWAHSnowflakeUploader
from ewah.uploaders.postgres import EWAHPostgresUploader
from ewah.uploaders.google_sheets import EWAHGSheetsUploader


def get_uploader(dwh_engine):
    try:
        return {
            EC.DWH_ENGINE_POSTGRES: EWAHPostgresUploader,
            EC.DWH_ENGINE_SNOWFLAKE: EWAHSnowflakeUploader,
            # DWH_ENGINE_BIGQUERY: bq_hook,
            EC.DWH_ENGINE_GS: EWAHGSheetsUploader,
        }[dwh_engine]
    except KeyError:
        raise Exception("Invalid Engine operator selected!")
