import os
from airflow import DAG  # This module must be imported for airflow to see DAGs
from airflow.configuration import conf

from ewah.dag_factories import dags_from_yml_file

file_name = "{0}{1}{2}".format(
    os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", conf.get("core", "dags_folder")),
    os.sep,
    "dags.yml",
)
if os.path.isfile(file_name):
    dags = dags_from_yml_file(file_name, True, True)
    for dag in dags:  # Must add the individual DAGs to the global namespace
        globals()[dag._dag_id] = dag
else:
    raise Exception("Not a file: {0}".format(file_name))
