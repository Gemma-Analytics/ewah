from ewah.constants import EWAHConstants as EC
from ewah.dwhooks.dwhook_snowflake import EWAHDWHookSnowflake
from ewah.dwhooks.dwhook_postgres import EWAHDWHookPostgres

def get_dwhook(dwh_engine):
    try:
        return {
            EC.DWH_ENGINE_POSTGRES: EWAHDWHookPostgres,
            EC.DWH_ENGINE_SNOWFLAKE: EWAHDWHookSnowflake,
            # DWH_ENGINE_BIGQUERY: bq_hook,
            # DWH_ENGINE_REDSHIFT: rs_hook,
        }[dwh_engine]
    except KeyError:
        raise Exception('Invalid Engine operator selected!')
