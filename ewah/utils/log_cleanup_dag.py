# Adapted from https://github.com/teamclairvoyant/airflow-maintenance-dags
from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.configuration import conf
from datetime import datetime, timedelta
import os

# from c_utils.various_constants import AirflowConstants
# acs = AirflowConstants()

"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup

--conf options:
    maxLogAgeInDays:<INT> - Optional

"""


def cleanup_dag_factory(
    dag_name="utils_airflow_log_cleanup",
    state_date=datetime(2018, 8, 23),
    schedule_interval=timedelta(days=1),
    max_log_age_in_days=7,
    dag_owner="utils",
    alert_emails=[],
):
    DAG_ID = dag_name  # os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-log-cleanup
    START_DATE = state_date
    BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER")
    SCHEDULE_INTERVAL = schedule_interval
    DAG_OWNER_NAME = (
        dag_owner  # Who is listed as the owner of this DAG in the Airflow Web Server
    )
    ALERT_EMAIL_ADDRESSES = (
        alert_emails  # List of email address to send email alerts to if this job fails
    )
    DEFAULT_MAX_LOG_AGE_IN_DAYS = max_log_age_in_days  # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or odler
    ENABLE_DELETE = True  # Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs
    NUMBER_OF_WORKERS = 1  # The number of worker nodes you have in Airflow. Will attempt to run this process for however many workers there are so that each worker gets its logs cleared.

    default_args = {
        "email": ALERT_EMAIL_ADDRESSES,
        "email_on_failure": bool(ALERT_EMAIL_ADDRESSES),
        "owner": dag_owner,
        "email_on_retry": False,
        "start_date": START_DATE,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    }

    dag = DAG(
        DAG_ID,
        default_args=default_args,
        schedule_interval=SCHEDULE_INTERVAL,
        start_date=START_DATE,
        catchup=False,
    )

    log_cleanup = (
        """
    echo "Getting Configurations..."
    BASE_LOG_FOLDER='"""
        + BASE_LOG_FOLDER
        + """'
    MAX_LOG_AGE_IN_DAYS=$maxLogAgeInDays
    ENABLE_DELETE="""
        + str("true" if ENABLE_DELETE else "false")
        + """
    echo "Finished Getting Configurations"
    echo ""

    echo "Configurations:"
    echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
    echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
    echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"
    echo ""

    echo "Running Cleanup Process..."
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
    echo "Executing Find Statement: ${FIND_STATEMENT}"
    FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`
    echo "Process will be Deleting the following directories:"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l ` file(s)"     # "grep -v '^$'" - removes empty lines. "wc -l" - Counts the number of lines
    echo ""

    if [ "${ENABLE_DELETE}" == "true" ];
    then
        DELETE_STMT="${FIND_STATEMENT} -delete"
        echo "Executing Delete Statement: ${DELETE_STMT}"
        eval ${DELETE_STMT}
        DELETE_STMT_EXIT_CODE=$?
        if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
            echo "Delete process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
            exit ${DELETE_STMT_EXIT_CODE}
        fi
    else
        echo "WARN: You're opted to skip deleting the files!!!"
    fi
    echo "Finished Running Cleanup Process"
    """
    )

    for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

        log_cleanup = BashOperator(
            task_id="log_cleanup_" + str(log_cleanup_id),
            bash_command=log_cleanup,
            dag=dag,
            env={
                "maxLogAgeInDays": '{{{{ dag_run.conf["maxLogAgeInDays"] if dag_run and dag_run.conf.get("maxLogAgeInDays") else {age} }}}}'.format(
                    age=str(DEFAULT_MAX_LOG_AGE_IN_DAYS)
                )
            },
        )

    return dag
