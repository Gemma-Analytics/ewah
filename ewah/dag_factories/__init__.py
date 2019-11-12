from ewah.constants import EWAHConstants as EC
from ewah.operators import *

from ewah.dag_factories.dag_factory_full_refresh import dag_factory_drop_and_replace
from ewah.dag_factories.dag_factory_incremental import dag_factory_incremental_loading

from copy import deepcopy

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

def dags_from_dict(
    dag_definition,
    raise_error_when_empty=False,
    raise_warnings_as_errors=False,
):
    """This function returns a list of airflow DAGs as specified by a dictionary
    containing all configuration of the DAGs.

    :param dag_definition:  Dictionary containing all configuration of the DAGs.
    :type dag_definition:   Dict
    :param raise_error_when_empty:  If no DAGs are configured, raise an error?
    :type raise_error_when_empty:   Boolean
    :param raise_warnings_as_errors:    If true, warnings become errors.
    :type raise_warnings_as_errors:     Boolean

    Levels of the dictionary:
    1) The first level may be one of the following:
    - el_dags: contains the actual DAG definitions
    - base_config: template configuration for all DAGs - will be overwritten
        by individual configurations

    el_dags is expected to be a dictionary where the key is the (base) name of
        the DAG(s) and the value is a dictionary of its configuration.
    Configuration options:
        - incremental   is this incremental loading? Defaults to False
        - dwh_engine    What engine does it run on? Possible values:
            postgres, snowflake
        - dwh_conn_id   airflow connection name (id) for the target DWH
        - el_operator   name of the EWAH EL Operator, one of: fx, ga, mysql,
            oracle, postgres, s3
        - start_date    DAG start date
        - end_date      DAG end date
        - target_schema_name    name of the schema to load data into (attn:
            only one schema per DAG! Is not automatically checked!)
        - target_schema_suffix  optional, temporary suffix for replacement
            schemas while loading new data
        - target_database_name optional for snowflake DWHs, forbidden otherwise
            -> name of the target database (looks at dwh_conn_id's extra json
            if not specified for a snowflake DWH)
        - default_args the default_args parameter as supplied to airflow DAGs
            if specified at DAG level, it overrides any base_config
        - operator_config this is complex enough to warrant its own section

        for drop and replace only:
        - schedule_interval only for drop and replace: schedule interval of DAG
            - defaults to timedelta(days=1), give as a dict containing
            interval(s), e.g. days=1 or hours=3

        for incremental loading only:
        - airflow_conn_id airflow connection name (id) for the airflow metadata
            database
        - schedule_interval_backfill the schedule interval for the backfill DAG
            timedelta - use this notation:
            schedule_interval: !!python/object/apply:datetime.timedelta
                - days go first
                - seconds go second
                - microseconds go third
        - schedule_interval_future the schedule interval for the normal DAG
            also a timedelta
        - switch_date the date to switch from backfill to normal DAG - if
            ommitted, it is calculated automatically based on start_date
            and the schedule intervals to be the latest possible date
            that is still in the past

        The operator_config: This is where arguments are specified that are
            related to the specific operator in use. There are four parts,
            two of which are always relevant and two are unique to incremental
            DAGs: general_config, incremental_config, backfill_config and tables
        general_config
            Is at the lowest level of the four and all arguments here can be
            overwritten at one of the other levels. Usually, this is where one
            specifies a source_conn_id or data_from and data_until
        incremental_config
            This is config to apply only to normal DAGs but not backfill DAGs
            for incremental DAG sets. Overwrites general_config, is overwritten
            by table config
        backfill_config
            Same as incremental_config, but only applies to the backfill DAGs
        tables
            Is a non-empty list of dicts, where each dict contains at least one
            key-value pair: "name", which is the name of the table in the DWH.
            May also contain any argument to overwrite the other operator config
            values as well as the arguments "skip_backfill", which removes this
            particular table from backfill DAGs, and "drop_and_replace",
            which is False per default but may be useful as True for some
            tables, especially used in conjunction with "skip_backfill"
    """

    _INCR_ONLY = [
        'airflow_conn_id',
        'schedule_interval_backfill',
        'schedule_interval_future',
        'switch_date',
    ]
    _FR_ONLY = ['schedule_interval']

    dwh_engines = {
        'postgres': EC.DWH_ENGINE_POSTGRES,
        'snowflake': EC.DWH_ENGINE_SNOWFLAKE,
        # 'bigquery': EC.DWH_ENGINE_BIGQUERY,
        # 'redshift': EC.DWH_ENGINE_REDSHIFT,
        # 's3': EC.DWH_ENGINE_S3,
    }

    operators = {
        'fx': EWAHFXOperator,
        'ga': EWAHGAOperator,
        'google_analytics': EWAHGAOperator,
        'mysql': EWAHMySQLOperator,
        'oracle': EWAHOracleSQLOperator,
        'postgres': EWAHPostgresOperator,
        's3': EWAHS3Operator,
    }

    def warn_me(*warning_strings):
        if raise_warnings_as_errors:
            raise Exception(' '.join(warning_strings))
        print(*warning_strings)

    base_config = dag_definition.pop('base_config', {})
    dags_dict = dag_definition.pop('el_dags', {})
    if not dags_dict:
        warn_me('No EL DAGs specified!')

    if dag_definition:
        warn_me(
            'Received unexpected arguments:',
            '\n{0}'.format(str(dag_definition)),
        )

    dags = []
    for dag_name, dag_config in dags_dict.items():
        config = deepcopy(base_config)
        config.update(dag_config)
        drop_and_replace = not config.pop('incremental', False)
        try:
            config.update({'dwh_engine': dwh_engines[config['dwh_engine']]})
        except KeyError:
            raise Exception('Invalid dwh_engine {0}\nAllowed:\n\t{1}'.format(
                str(config.get('dwh_engine', None)),
                '\n\t'.join(list(dwh_engines.keys())),
            ))
        try:
            config.update({'el_operator': operators[config['el_operator']]})
        except KeyError:
            raise Exception('Invalid el_operator {0}\nAllowed:\n\t{1}'.format(
                str(config.get('el_operator', None)),
                '\n\t'.join(list(operators.keys())),
            ))

        tables_dict = {}
        for _ in range(len(config['operator_config']['tables'])):
            table = config['operator_config']['tables'].pop(0)
            table_name = table.pop('name')
            tables_dict.update({table_name: table})
        config['operator_config']['tables'] = tables_dict

        if drop_and_replace:
            config.update({'dag_name': dag_name})
            for key in _INCR_ONLY: # allow specifying everything in base config
                config.pop(key, '')
            dags += [dag_factory_drop_and_replace(**config)]
        else:
            for key in _FR_ONLY:
                config.pop(key, '')
            config.update({'dag_base_name': dag_name})
            dags += list(dag_factory_incremental_loading(**config))
    if dags:
        return dags
    else:
        warn_me('No DAGs created!')
        return []

def dags_from_yml_file(
    file_path,
    raise_error_when_empty=False,
    raise_warnings_as_errors=False,
):

    return dags_from_dict(
        dag_definition=yaml.load(open(file_path, 'r'), Loader=Loader),
        raise_error_when_empty=raise_error_when_empty,
        raise_warnings_as_errors=raise_warnings_as_errors,
    )
