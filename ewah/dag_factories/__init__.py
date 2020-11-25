from ewah.constants import EWAHConstants as EC
from ewah.operators import *
from ewah.operators.base_operator import EWAHBaseOperator as EBO

from .dag_factory_full_refresh import dag_factory_drop_and_replace
from .dag_factory_fullcremental import dag_factory_fullcremental
from .dag_factory_incremental import dag_factory_incremental_loading

from copy import deepcopy

import yaml
from ewah.ewah_utils.yml_loader import Loader, Dumper

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
        by individual configurations, except for additional_task_args, which
        will only be updated by DAG configurations

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
        - read_right_users  comma-separated string or list of users or roles
            to receive read rights on the created schemas and tables.
        - target_schema_name    name of the schema to load data into (attn:
            only one schema per DAG! Is not automatically checked!)
        - target_schema_suffix  optional, temporary suffix for replacement
            schemas while loading new data
        - target_database_name optional for snowflake DWHs, forbidden otherwise
            -> name of the target database (looks at dwh_conn_id's extra json
            if not specified for a snowflake DWH)
        - additional_task_args optional dict with arguments for the individual
            tasks of the DAG(s). Allowed arguments are:
            - email
            - email_on_retry
            - email_on_failure
            - retries
            - retry_delay
            - priorirty_weight
            - weight_rule
            - pool
            - owner
            ->> See airflow documentation for details
        - additional_dag_args optional dict, you can additionally set the
            following DAG-level arguments in this dict:
            - description
            - params
            - default_view
            - orientation
            - access_control
            ->> See airflow documentation for details
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
            Is a dict of dicts, where the key of the dict is the name of the
            table in the DWH. May also contain any argument to overwrite the
            other operator config values as well as the arguments
            "skip_backfill", which removes this particular table from backfill
            DAGs, and "drop_and_replace", which is False per default but may
            be useful as True for some tables, especially used in conjunction
            with "skip_backfill"
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
        'Postgres': EC.DWH_ENGINE_POSTGRES,
        'snowflake': EC.DWH_ENGINE_SNOWFLAKE,
        'Snowflake': EC.DWH_ENGINE_SNOWFLAKE,
        'sheets': EC.DWH_ENGINE_GS,
        'gsheets': EC.DWH_ENGINE_GS,
        'GSheets': EC.DWH_ENGINE_GS,
        'Google Sheets': EC.DWH_ENGINE_GS,
        EC.DWH_ENGINE_POSTGRES: EC.DWH_ENGINE_POSTGRES,
        EC.DWH_ENGINE_SNOWFLAKE: EC.DWH_ENGINE_SNOWFLAKE,
        EC.DWH_ENGINE_GS: EC.DWH_ENGINE_GS,
        # EC.DWH_ENGINE_BIGQUERY: EC.DWH_ENGINE_BIGQUERY,
        # EC.DWH_ENGINE_REDSHIFT: EC.DWH_ENGINE_REDSHIFT,
        # EC.DWH_ENGINE_S3: EC.DWH_ENGINE_S3,
        # 'bigquery': EC.DWH_ENGINE_BIGQUERY,
        # 'redshift': EC.DWH_ENGINE_REDSHIFT,
        # 's3': EC.DWH_ENGINE_S3,
    }

    operators = {
        'fx': EWAHFXOperator,
        'ga': EWAHGAOperator,
        'google_analytics': EWAHGAOperator,
        'gads': EWAHGoogleAdsOperator,
        'google_ads': EWAHGoogleAdsOperator,
        'mysql': EWAHMySQLOperator,
        'oracle': EWAHOracleSQLOperator,
        'postgres': EWAHPostgresOperator,
        'postgresql': EWAHPostgresOperator,
        'pgsql': EWAHPostgresOperator,
        's3': EWAHS3Operator,
        'fb': EWAHFBOperator,
        'facebook': EWAHFBOperator,
        'gsheets': EWAHGSpreadOperator,
        'google_sheets': EWAHGSpreadOperator,
        'gs': EWAHGSpreadOperator,
        'mongo': EWAHMongoDBOperator,
        'mongodb': EWAHMongoDBOperator,
        'shopify': EWAHShopifyOperator,
        'zendesk': EWAHZendeskOperator,
        'gmaps': EWAHGMapsOperator,
        'googlemaps': EWAHGMapsOperator,
        'mc': EWAHMailchimpOperator,
        'mailchimp': EWAHMailchimpOperator,
        'bq': EWAHBigQueryOperator,
        'bigquery': EWAHBigQueryOperator,
        'hubspot': EWAHHubspotOperator,
    }

    allowed_dag_args = [
        'description',
        'params',
        'default_view',
        'orientation',
        'access_control',
        'concurrency'
    ]

    allowed_task_args = [
        'email',
        'email_on_retry',
        'email_on_failure',
        'retries',
        'retry_delay',
        'priorirty_weight',
        'weight_rule',
        'pool',
        'owner', # set within this function, can be overwritten
    ]

    factory_type = {
        'fr': dag_factory_drop_and_replace,
        'full-refresh': dag_factory_drop_and_replace,
        'full refresh': dag_factory_drop_and_replace,
        'fullcremental': dag_factory_fullcremental,
        'periodic fr': dag_factory_fullcremental,
        'inc': dag_factory_incremental_loading,
        'incr': dag_factory_incremental_loading,
        'incremental': dag_factory_incremental_loading,
    }

    def warn_me(*warning_strings):
        if raise_warnings_as_errors:
            raise Exception(' '.join(warning_strings))
        print(*warning_strings)

    def check_keys_are_allowed(dict, allowed_list, dict_name):
        for key in dict.keys():
            if not key in allowed_list:
                warn_me('Unexpected key {0} was found in dict {1}!'.format(
                    key,
                    dict_name,
                ))

    base_config = dag_definition.pop('base_config', {})
    dags_dict = dag_definition.pop('el_dags', {})
    if not dags_dict:
        warn_me('No EL DAGs specified!')

    additional_dag_args = base_config.pop('additional_dag_args', {})
    base_config['additional_dag_args'] = {}
    for key, value in additional_dag_args.items():
        if key in allowed_dag_args:
            base_config['additional_dag_args'].update({key: value})
        else:
            warn_me(('Unexpected key {0} found in ' \
                + 'additional_dag_args in base_config!').format(
                key,
            ))

    additional_task_args = base_config.pop('additional_task_args', {})
    base_config['additional_task_args'] = {'owner': 'EWAH'}
    for key, value in additional_task_args.items():
        if key in allowed_task_args:
            base_config['additional_task_args'].update({key: value})
        else:
            warn_me('Unexpected key {0} found in additional_task_args!'.format(
                key,
            ))

    if dag_definition:
        warn_me(
            'Received unexpected arguments:',
            '\n{0}'.format(str(dag_definition)),
        )

    dags = []
    for dag_name, dag_config in dags_dict.items():
        config = deepcopy(base_config)
        config['additional_task_args'].update(
            dag_config.pop('additional_task_args', {})
        )
        config['additional_dag_args'].update(
            dag_config.pop('additional_dag_args', {})
        )
        check_keys_are_allowed(
            config['additional_task_args'],
            allowed_task_args,
            'el_dags.{0}.additional_task_args'.format(dag_name),
        )
        check_keys_are_allowed(
            config['additional_dag_args'],
            allowed_dag_args,
            'el_dags.{0}.additional_dag_args'.format(dag_name),
        )
        config.update(dag_config)
        load_strategy_depr = config.pop('incremental', None)
        load_strategy = config.pop('load_strategy', None)
        if (not load_strategy_depr is None) and load_strategy:
            _msg = 'incremental kwarg is depcrecated and cannot be used '
            _msg += 'simultaneously with load_strategy!'
            raise Exception(_msg)
        if load_strategy is None and load_strategy_depr is None:
            _msg = 'Must supply kwarg load_strategy!'
            raise Exception(_msg)
        if not load_strategy_depr is None:
            if load_strategy_depr:
                load_strategy = 'inc'
            else:
                load_strategy = 'fr'
        dag_factory = factory_type.get(load_strategy.lower())
        if not dag_factory:
            _msg = 'load_strategy={0} is invalid!'.format(load_strategy)
            raise Exception(_msg)

        try:
            config.update({'dwh_engine': dwh_engines[config['dwh_engine']]})
        except KeyError:
            raise Exception('Invalid dwh_engine {0}\nAllowed:\n\t{1}'.format(
                str(config.get('dwh_engine', None)),
                '\n\t'.join(list(dwh_engines.keys())),
            ))
        try:
            if type(config['el_operator']) == type \
                and issubclass(config['el_operator'], EBO):
                # custom operator was defined in yml file, use it
                config.update({
                    'el_operator': config['el_operator'],
                })
            else:
                config.update({
                    'el_operator': operators[config['el_operator'].lower()],
                })
        except KeyError:
            raise Exception('Invalid el_operator {0}\nAllowed:\n\t{1}'.format(
                str(config.get('el_operator', None)),
                '\n\t'.join(list(operators.keys())),
            ))


        if dag_factory == dag_factory_drop_and_replace:
            config.update({'dag_name': dag_name})
            for key in _INCR_ONLY: # allow specifying everything in base config
                config.pop(key, '')
            dags += [dag_factory(**config)]
        elif dag_factory == dag_factory_incremental_loading:
            for key in _FR_ONLY:
                config.pop(key, '')
            config.update({'dag_base_name': dag_name})
            dags += list(dag_factory(**config))
        else:
            config.update({'dag_base_name': dag_name})
            dags += list(dag_factory(**config))

    if not dags:
        warn_me('No DAGs created!')

    return dags

def dags_from_yml_file(
    file_path,
    raise_error_when_empty=True,
    raise_warnings_as_errors=True,
):

    return dags_from_dict(
        dag_definition=yaml.load(open(file_path, 'r'), Loader=Loader),
        raise_error_when_empty=raise_error_when_empty,
        raise_warnings_as_errors=raise_warnings_as_errors,
    )
