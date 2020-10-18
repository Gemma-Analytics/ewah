#!/bin/bash

airflow upgradedb && \
  pip install --user --upgrade -e /opt/ewah && \
  python /opt/airflow/docker/add_conns.py && \
  airflow webserver
