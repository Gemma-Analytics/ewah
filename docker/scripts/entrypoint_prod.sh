#!/usr/bin/env bash
# This script extends the original airflow image in 2 ways:
# 1) always run airflow upgradedb to have a current metadata database
# 2) If the command is for local development of EWAH, install via bind-mounted
#   volume. If it's not, it will be using an image with EWAH installed from pip.
# 3) If desired (e.g. for development purposes), run a script for the following:
#   - add a default superuser to the backend, if applicable
#   - remove the length limit in the extra column of airflow's connections
#   - add connections found in an `airflow_connections.yml`


AIRFLOW_COMMAND="${1}"

# 1)
if [[ ${AIRFLOW_COMMAND} == "webserver" ]]; then
  # ensure metadata database is up to date
  echo -e "\n\nUpgrading Metadata database:\n\n"
  airflow upgradedb
fi

# 2)
if [[ ${EWAH_IMAGE_TYPE} == "DEV" ]]; then
  # install ewah from bind-mounted volume in /opt/ewah if development env
  echo -e "\n\nInstalling EWAH from bind-mounted volume:\n\n"
  pip install --user --upgrade -e /opt/ewah
fi

# 3)
if [[ ${AIRFLOW_COMMAND} == "webserver" ]]; then
  echo -e "\n\nIf applicable, run support scripts:"
  if [[ ${EWAH_RUN_DEV_SUPPORT_SCRIPTS} == "1" ]]; then
    echo -e "\n\n\tIndeed, run support scripts!\n\n"
    python /entrypoint.py
  else
    echo -e "\n\n\tNope, not running the scripts!\n\n"
  fi
fi

# Run the actual command
exec airflow "${@}"
