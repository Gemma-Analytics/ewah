import os
from airflow.configuration import conf

from ewah.dag_factories import dags_from_yml_file

dags_file = "dags.yml"
for dag in dags_from_yml_file(conf.get("core", "dags_folder") + os.sep + dags_file):
    # Must add the individual DAGs to the global namespace,
    # otherwise airflow does not find the DAGs!
    globals()[dag._dag_id] = dag
