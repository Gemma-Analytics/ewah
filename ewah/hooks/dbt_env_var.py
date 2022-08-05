from ewah.hooks.base import EWAHBaseHook


class EWAHdbtEnvVarHook(EWAHBaseHook):
    """
    Very simple hook to show a custom connection type in Airflow.
    Not used for data loading.
    Use it to create environment variable secrets using Airflow connections
    which are then used in dbt runs. Usage may be extended to other use cases
    of environment variables in the future.
    """

    _ATTR_RELABEL = {}

    conn_name_attr = "ewah_dbt_env_var_conn_id"
    default_conn_name = "ewah_dbt_env_var_default"
    conn_type = "ewah_dbt_env_var"
    hook_name = "EWAH dbt Environment Variable"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["extra", "port", "host", "schema"],
            "relabeling": {
                "login": "Environment Variable Name",
                "password": "Environment Variable Value",
            },
        }
