# ewah
Ewah: ELT With Airflow Helper - Classes and functions to make apache airflow life easier.

Functions to create all DAGs required for ELT using only a simple config file.

## DWHs Implemented
- Snowflake
- PostgreSQL

## DWHs Planned
- Bigquery

## Operators

EWAH currently supports the following operators:

- Aircall
- BigQuery
- DynamoDB
- Facebook (partially, so far: ads insights; incremental only)
- FX Rates (from Yahoo Finance)
- Google Ads
- Google Analytics (incremental only)
- Google Maps (location data from an address)
- Google Sheets
- Hubspot
- Mailchimp
- Mailingwork
- MongoDB
- MySQL
- OracleSQL
- Pipedrive
- PostgreSQL / Redshift
- Recurly
- S3 (for CSV or JSON files stored in an S3 bucket, e.g. from Kinesis Firehose)
- Salesforce
- Shopify
- Stripe
- Zendesk

### Universal operator arguments

The following arguments are accepted by all operators, unless explicitly stated otherwise:

| argument | required | type | default | description |
| --- | --- | --- | --- | --- |
| source_conn_id | yes | string | n.a. | name of the airflow connection with source credentials |
| dwh_engine | yes | string | n.a. | DWH type - e.g. postgres - usually |
| dwh_conn_id | yes | string | n.a. | name of the airflow connection of the DWH |
| target_table_name | implicit | string | n.a. | name of the table in the DWH; the target table name is the name given in the table config |
| target_schema_name | yes | string | name of the schema in the DWH where the table will live  |
| target_schema_name_suffix | no | string | `_next` | when loading new data, how to suffix the schema name during the loading process |
| target_database_name | yes for Snowflake DWH | string | n.a. | name of the database (only for Snowflake, illegal argument for non-Snowflake DWHs) |
| columns_definition | operator-dependent | dict | n.a. | usually not required; dictionary definition of the columns to get from a data source and their data types; if left blank, all data will be pulled and data types inferred |
| drop_and_replace | no | boolean | same as DAG-level setting | whether a table is loading as full refresh or incrementally. Normally set by the DAG level config. Incremental loads can overwrite this setting to fully refresh some small tables (e.g. if they are small and have no `updated_at` column) |
| update_on_columns | operator-dependent | list of strings | n.a. | for incremental loading, update data on what set columns? (effectively, the list of columns comprising the composite primary key); usually not required |
| primary_key_column_name | operator-dependent | string | n.a. | name of the primary key column; if given, EWAH will set the column as primary key in the DWH; may also use this as alternatively of update_on_columns for some operators |
| clean_data_before_upload | no | boolean | True | Some minor clean up before data upload. Slows performance, but avoids some weird errors in special cases. |
| add_metadata | no | boolean | True | some operators may add metadata to the tables; this behavior can be turned off (e.g. shop name for the shopify operator) |

### Operator: Google Ads

These arguments are specific to the Google Ads operator. In addition, the Google Ads operator ignores any `update_on_columns` argument given, as it overwrites it with the the list of non-metric fields.

| argument | required | type | default | description |
| --- | --- | --- | --- | --- |
| fields | yes | dict | n.a. | most important argument; excludes metrics; detailed below |
| metrics | yes | list of strings | n.a. | list of all metrics to load, must load at least one metric |
| resource | yes | string | n.a. | name of the report, e.g. `keyword_view` |
| client_id | yes | string | n.a. | 10-digit number, often written with hyphens, e.g. `123-123-1234` (acceptable with or without hyphens) |
| conditions | no | list of strings | n.a. | list of strings of condition to include in the query, all conditions will be combined using `AND` operator |
| data_from | no | datetime, timedelta or airflow-template-string | execution_date of task instance | start date of particular airflow task instance OR timedelta -> calculate delta from data_until |
| data_until | no | datetime or airflow-template-string | next_execution_date of task instance | get data from google_ads until this point |

#### arguments: fields and metrics

the `fields` and `metrics` arguments are the most important for this operator. The `metrics` are separated from `fields` because the `fields` are simultaneously the updated_on_columns. When creating the google ads query, they are combined. The `metrics` argument is simply a list of metrics to be requested from Google Ads. The `fields` argument is a bit more complex, due to the nature of Google Ad's API. It is essentially a nested json.

Because the query may look something like this:
```sql
SELECT
    campaign.id
  , campaign.name
  , ad_group_criterion.criterion_id
  , ad_group_criterion.keyword.text
  , ad_group_criterion.keyword.match_type
  , segments.date
  , metrics.impressions
  , metrics.clicks
  , metrics.cost_micros
FROM keyword_view
WHERE segments.date BETWEEN 2020-08-01 AND 2020-08-08
```

i.e., there are nested object structures, the `fields` structure must reflect the same. Take a look at the example config for the correct configuration of abovementioned Google Ads query. Note in addition, that the fields will be uploaded with the same names to the DWH, excepts that the periods will be replaced by underscored. i.e., the table `keyword_view_data` in the example below will have the columns `campaign_id`, `ad_group_criterion_keyword_text`, etc.

Finally, note that `segments.date` is always required in the `fields` argument.

### Oracle operator particularities

The Oracle operator utilizes the `cx_Oracle` python library. To make it work, you need to install additional packages, see [here](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#installing-cx-oracle-on-linux) for details.

#### Example

Sample configuration in `dags.yaml` file:
```yaml
EL_Google_Ads:
  incremental: True
  el_operator: google_ads
  target_schema_name: raw_google_ads
  operator_config:
    general_config:
      source_conn_id: google_ads
      client_id: "123-123-1234"
      # conditions: none
    incremental_config:
      data_from: !!python/object/apply:datetime.timedelta
        - 3 # days as time interval before the execution date of the DAG
        # only use this for normal runs! backfills: use execution date context instead
    tables:
      keyword_view_data:
        resource: keyword_view
        fields:
          campaign:
            - id
            - name
          ad_group_criterion:
            - keyword:
              - text
              - match_type
            - criterion_id
          segments:
            - date
        metrics:
          - impressions
          - clicks
          - cost_micros
```


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
The former must be a child object of `ewah.operators.base.EWAHBaseOperator`. Ideally, The required operator is already available for your use. Please feel free to fork and commit your own operators to this project! The latter is a dictionary containing the entire configuration of the operator. This is where you define what tables to load, how to load them, if loading specific columns only, and any other detail related to your EL job.

### Full refresh factory

A `filename.py` file in your airflow/dags folder may look something like this:
```python
from ewah.ewah_utils.dag_factory_full_refresh import dag_factory_drop_and_replace
from ewah.constants import EWAHConstants as EC
from ewah.operators.postgres import EWAHPostgresOperator

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

## Developing EWAH locally with Docker

It is easy to develop EWAH with Docker. Here's how:

* Step 1: clone the repository locally
* Step 2: configure your local secrets
  * In `ewah/airflow/docker/secrets`, you should at least have a file called `secret_airflow_connections.yml` which looks exactly like `ewah/airflow/docker/airflow_connections.yml` and contains any additional credentials you may need during development -> this file can also contain connections defined in the `airflow_connections.yml`, in which case your connections will overwrite them.
  * You can add any other files in this folder that you may need, e.g. private key files
  * For example, you can add a file called `google_service_acc.json` that contains service account credentials that can be loaded as extra into an airflow connection used for a Google Analytics DAG
  * Sample files are shown below
* Step 3: run `docker-compose up` to start the postgres, webserver and scheduler containers
  * You can stop them with `CTRL+C`
  * If you want to free up the ports again or if you are generally done developing, you should additionally run `docker-compose down`
* Step 4: Make any changes you wish to files in `ewah/ewah/` or `airflow/dags/`
  * If you add new dependencies, makes sure to add them in the `ewah/setup.py` file; you may need to restart the containers to include the new dependencies in the ewah installations running in your containers
* Step 5: Commit, Push and open a PR
  * When pushing, feel free to include configurations in your `ewah/airflow/dags/dags.yml` that utilize your new feature, if applicable

##### Sample `secret_airflow_connections.yml`
```yaml
---

connections:
  - id: ssh_tunnel
    host: [SSH Server IP]
    port: [SSH Port] # usually 22
    login: [SSH username]
    password: [private key password OR SSH server password]
    extra: !text_from_file /opt/airflow/docker/secrets/ssh_rsa_private # example, with a file called `ssh_rsa_private` in your `ewah/airflow/docker/secrets/` folder
  - id: shopware
    host: [host] # "localhost" if tunnelling into the server running the MySQL database
    schema: [database_name]
    port: 3306
    login: [username]
    password: [password]
  - id: google_service_account
    extra: !text_from_file /opt/airflow/docker/secrets/google_service_acc.json # example, with a file called `google_service_acc.json` in your `ewah/airflow/docker/secrets/` folder

...
```

##### Sample `google_service_acc.json`
```json
{"client_secrets": {
  "type": "service_account",
  "project_id": "my-project-id",
  "private_key_id": "abcdefghij1234567890abcdefghij1234567890",
  "private_key": "-----BEGIN PRIVATE KEY-----\n[...]\n-----END PRIVATE KEY-----\n",
  "client_email": "xxx@my-project-id.iam.gserviceaccount.com",
  "client_id": "012345678901234567890",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xxx%40my-project-id.iam.gserviceaccount.com"
}}
```

## Using EWAH with Astronomer

To avoid all devops troubles, it is particularly easy to use EWAH with astronomer.
Your astronomer project requires the following:
- add `ewah` to the `requirements.txt`
- add `libstdc++` to the `packages.txt`
- have a `dags.py` file and a `dags.yml` file in your dags folder
- in production, you may need to request your airflow metadata postgres database password from the support for incremental loading DAGs
