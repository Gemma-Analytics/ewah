# ewah
Ewah: ELT With Airflow Helper - Classes and functions to make apache airflow life easier.

Pre-Alpha. Used by myself for specific usecases at the moment.

Goal: Have functions to create all DAGs required for ELT using only a simple config file. Use this as a basis to build a GUI on top of it.

## DWHs Implemented
- Snowflake
- PostgreSQL

## DWHs Planned
- Bigquery

## Operators Implemented
- PostgreSQL
- MySQL
- OracleSQL
- Google Analytics (incremental only)
- S3 (for JSON files stored in an S3 bucket, e.g. from Kinesis Firehose)
- FX Rates (from Yahoo Finance)
- Facebook (partially, so far: ads insights; incremental only)
- Google Sheets
- MongoDB

## Philosophy

This package strictly follows an ELT Philosophy:
- Business value is created by infusing business logic into the data and making great analyses and usable data available to stakeholders, not by building data pipelines
- Airflow solely orchestrates loading raw data into a central DWH
- Data is either loaded as full refresh (all data at every load) or incrementally, exploiting airflow's catchup and execution logic
- The only additional DAGs are dbt DAGs and utility DAGs
- Within that DWH, each data source lives in its own schema (e.g. `raw_salesforce`)
- Irrespective of full refresh or incremental loading, DAGs always load into a separate schema (e.g. `raw_salesforce_next`) and at the end replace the schema with the old data with the schema with the new data, to avoid data corruption due to errors in DAG execution
- Any data transformation is defined using SQL, ideally using [dbt](https://github.com/fishtown-analytics/dbt)
- Seriously, dbt is awesome, give it a shot!
- *(Non-SQL) Code contains no transformations*

## Usage

In your airflow Dags folder, define the DAGs by invoking either the incremental loading or full refresh DAG factory. The incremental loading DAG factory returns three DAGs in a tuple, make sure to call it like so: `dag1, dag2, dag3 = dag_factory_incremental_loading()` or add the dag IDs to your namespace like so:
```python
dags = dag_factory_incremental_loading()
for dag in dags:
  globals()[dag._dag_id] = dag
```
Otherwise, airflow will not recognize the DAGs. Most arguments should be self-explanatory. The two noteworthy arguments are `el_operator` and `operator_config`.
The former must be a child object of `ewah.operators.base_operator.EWAHBaseOperator`. Ideally, The required operator is already available for your use. Please feel free to fork and commit your own operators to this project! The latter is a dictionary containing the entire configuration of the operator. This is where you define what tables to load, how to load them, if loading specific columns only, and any other detail related to your EL job.

### Full refresh factory

A `filename.py` file in your airflow/dags folder may look something like this:
```python
from ewah.ewah_utils.dag_factory_full_refresh import dag_factory_drop_and_replace
from ewah.constants import EWAHConstants as EC
from ewah.operators.postgres_operator import EWAHPostgresOperator

from datetime import datetime, timedelta

dag = dag_factory_drop_and_replace(
    dag_name='EL_production_postgres_database', # Name of the DAG
    dwh_engine=EC.DWH_ENGINE_POSTGRES, # Implemented DWH Engine
    dwh_conn_id='dwh', # Airflow connection ID with connection details to the DWH
    el_operator=EWAHPostgresOperator, # Ewah Operator (or custom child class of EWAHBaseOperator)
    target_schema_name='raw_production', # Name of the raw schema where data will end up in the DWH
    target_schema_suffix='_next', # suffix of the schema containing the data before replacing the production data schema with the temporary loading schema
    # target_database_name='raw', # Only Snowflake
    start_date=datetime(2019, 10, 23), # As per airflow standard
    schedule_interval=timedelta(hours=1), # Only timedelta is allowed!
    default_args={ # Default args for DAG as per airflow standard
        'owner': 'Data Engineering',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_retry': False,
        'email_on_failure': True,
        'email': ['email@address.com'],
    },
    operator_config={
        'general_config': {
            'source_conn_id': 'production_postgres',
            'source_schema_name': 'public',
        },
        'tables': {
            'table_name':{},
            # ...
            # Additional optional kwargs at the table level:
            #   columns_definition
            #   update_on_columns
            #   primary_key_column_name
            #   + any operator specific arguments
        },
    },
)
```

For all kwargs of the operator config, the general config can be overwritten by supplying specific kwargs at the table level.

### Configure all DAGs in a single YAML file

Standard data loading DAGs should be just a configuration. Thus, you can
configure the DAGs using a simple YAML file. Your `dags.py` file in your
`$AIRFLOW_HOME/dags` folder may then look like that, and nothing more:
```python
import os
from airflow import DAG # This module must be imported for airflow to see DAGs
from airflow.configuration import conf

from ewah.dag_factories import dags_from_yml_file

folder = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER', None)
folder = folder or conf.get("core", "dags_folder")
dags = dags_from_yml_file(folder + os.sep + 'dags.yml', True, True)
for dag in dags: # Must add the individual DAGs to the global namespace
    globals()[dag._dag_id] = dag

```
And the YAML file may look like this:
```YAML
---

base_config: # applied to all DAGs unless overwritten
  dwh_engine: postgres
  dwh_conn_id: dwh
  airflow_conn_id: airflow
  start_date: 2019-10-23 00:00:00+00:00
  schedule_interval: !!python/object/apply:datetime.timedelta
    - 0 # days
    - 3600 # seconds
  schedule_interval_backfill: !!python/object/apply:datetime.timedelta
    - 7
  schedule_interval_future: !!python/object/apply:datetime.timedelta
    - 0
    - 3600
  additional_task_args:
    retries: 1
    retry_delay: !!python/object/apply:datetime.timedelta
      - 0
      - 300
    email_on_retry: False
    email_on_failure: True
    email: ['me+airflowerror@mail.com']
el_dags:
  EL_Production: # equals the name of the DAG
    incremental: False
    el_operator: postgres
    target_schema_name: raw_production
    operator_config:
      general_config:
        source_conn_id: production_postgres
        source_schema_name: public
      tables:
        users:
          source_table_name: Users
        transactions:
          source_table_name: UserTransactions
          source_schema_name: transaction_schema # Overwrite general_config args as needed
  EL_Facebook:
    incremental: True
    el_operator: fb
    start_date: 2019-07-01 00:00:00+00:00
    target_schema_name: raw_facebook
    operator_config:
      general_config:
        source_conn_id: facebook
        account_ids:
          - 123
          - 987
        data_from: '{{ execution_date }}' # Some fields allow airflow templating, depending on the operator
        data_until: '{{ next_execution_date }}'
        level: ad
      tables:
        ads_data_age_gender:
          insight_fields:
            - adset_id
            - adset_name
            - campaign_name
            - campaign_id
            - spend
          breackdowns:
            - age
            - gender
...
```

## Using EWAH with Astronomer

To avoid all devops troubles, it is particularly easy to use EWAH with astronomer.
Your astronomer project requires the following:
- add `ewah` to the `requirements.txt`
- add `libstdc++` to the `packages.txt`
- have a `dags.py` file and a `dags.yml` file in your dags folder
- in production, you may need to request your airflow metadata postgres database password from the support for incremental loading DAGs
