FROM apache/airflow

RUN pip install --user --upgrade pip

USER root
# required to install psycopg2 which is a dependency of ewah
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-server-dev-all gcc
# create folder and give user airflow sufficient access rights
RUN mkdir /opt/ewah && \
    chmod -R 777 /opt/ewah
USER airflow

# install psycopg2 - optional, but increases iteration speed
RUN pip install --user --upgrade psycopg2
