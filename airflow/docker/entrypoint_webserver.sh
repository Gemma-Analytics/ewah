#!/bin/bash

airflow upgradedb && \
  pip install --user --upgrade -e /opt/ewah && \
  python /opt/airflow/docker/remove_extra_length_constraint.py && \
  python /opt/airflow/docker/add_conns.py && \
  python /opt/airflow/docker/add_admin.py && \
  airflow webserver
