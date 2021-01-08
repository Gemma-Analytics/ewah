from airflow.operators.bash_operator import BashOperator
from ewah.ewah_utils.log_cleanup_dag import cleanup_dag_factory

email = ["me+airflowerror@email.com"]

# airflow logs
dag_airflow_logs = cleanup_dag_factory(
    dag_name="Delete_airflow_logs",
    max_log_age_in_days=14,
    dag_owner="utils",
    alert_emails=email,
)
