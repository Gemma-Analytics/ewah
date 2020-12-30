import os
from airflow import DAG  # This module must be imported for airflow to see DAGs
from airflow.configuration import conf

from ewah.dag_factories import dags_from_yml_file

folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", None)
folder = folder or conf.get("core", "dags_folder")
dags = dags_from_yml_file(folder + os.sep + "dags.yml", True, True)
for dag in dags:  # Must add the individual DAGs to the global namespace
    globals()[dag._dag_id] = dag
