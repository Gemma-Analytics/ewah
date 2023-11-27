from airflow import DAG
from airflow.operators.bash import BashOperator


def git_pull_dag_factory(
    dag_name,
    folder,
    default_args={},
    start_date=None,
    schedule_interval=None,
):
    dag = DAG(
        dag_name,
        catchup=False,
        max_active_runs=1,
        schedule_interval=schedule_interval,
        start_date=start_date,
        default_args=default_args,
    )

    task = BashOperator(
        task_id="git_pull_task",
        bash_command="cd {0} && git pull".format(folder),
        dag=dag,
    )

    return dag
