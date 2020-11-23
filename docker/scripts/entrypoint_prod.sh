#!/usr/bin/env bash
# If the command is webserver, run a few scripts first!

AIRFLOW_COMMAND="${1}"

if [[ ${AIRFLOW_COMMAND} == "webserver" ]]; then
  # ensure metadata database is up to date
  airflow upgradedb
fi

if [[ ${EWAH_IMAGE_TYPE} == "DEV" ]]; then
  # install ewah from bind-mounted volume in /opt/ewah if development env
  pip install --user --upgrade -e /opt/ewah
fi

if [[ ${AIRFLOW_COMMAND} == "webserver" ]]; then
  # run additional scripts
  python /entrypoint.py
fi

# Run the command
exec airflow "${@}"
