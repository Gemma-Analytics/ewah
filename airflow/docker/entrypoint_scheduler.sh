#!/bin/bash

pip install --user --upgrade -e /opt/ewah && \
  airflow scheduler
