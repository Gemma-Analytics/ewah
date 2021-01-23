#!/usr/bin/env bash
# This script extends the original airflow image in 3 ways:
# 1) always run airflow upgradedb to have a current metadata database
# 2) If the command is for local development of EWAH, install via bind-mounted
#   volume. If it's not, it will be using an image with EWAH installed from pip.
# 3) If desired (e.g. for development purposes), run a script for the following:
#   - add a default superuser to the backend, if applicable
#   - remove the length limit in the extra column of airflow's connections
#   - add connections found in an `airflow_connections.yml`
#
# Note: If the command is "all", then run both scheduler and webserver in parallel


AIRFLOW_COMMAND="${1}"

# Always remove the airflow.cfg file that airflow creates when running
rm -r -f /opt/airflow/airflow.cfg

# 1)
if [[ ${AIRFLOW_COMMAND} == "webserver" ]] || [[ ${AIRFLOW_COMMAND} == "all" ]]; then
  # ensure metadata database is up to date
  echo -e "\n\nUpgrading Metadata database:\n\n"
  airflow db upgrade
fi

# 2)
if [[ ${EWAH_IMAGE_TYPE} == "DEV" ]]; then
  # install ewah from bind-mounted volume in /opt/ewah if development env
  echo -e "\n\nInstalling EWAH from bind-mounted volume:\n\n"
  pip install --user --upgrade -e /opt/ewah
fi

# 3)
if [[ ${AIRFLOW_COMMAND} == "webserver" ]] || [[ ${AIRFLOW_COMMAND} == "all" ]]; then
  if [[ ${EWAH_RUN_DEV_SUPPORT_SCRIPTS} == "1" ]]; then
    python /entrypoint.py
    if [[ ${EWAH_AIRFLOW_USER_SET} == "1" ]]; then
      echo -e "\n\n\n\nCreating RBAC admin user...\n\n"
      airflow users create \
        --role Admin \
        --username $EWAH_AIRFLOW_USER_USER \
        --firstname $EWAH_AIRFLOW_USER_FIRSTNAME \
        --lastname $EWAH_AIRFLOW_USER_LASTNAME \
        --email $EWAH_AIRFLOW_USER_EMAIL \
        --password $EWAH_AIRFLOW_USER_PASSWORD
      echo -e "\n\n"
    fi
  fi
fi

# Run the actual command
if [[ ${AIRFLOW_COMMAND} == "all" ]]; then
  # run both scheduler and webserver in parallel in the same container
  exec airflow scheduler & airflow webserver
else
  exec airflow "${@}"
fi
