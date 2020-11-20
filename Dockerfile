FROM apache/airflow

RUN pip install --user --upgrade pip

USER root

# required packages to install psycopg2 which is a dependency of ewah
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-server-dev-all gcc

# create folder and give user airflow sufficient access rights
RUN mkdir /opt/ewah && \
    chmod -R 777 /opt/ewah

# required to run git with SSH
RUN apt-get install -y --no-install-recommends git

# install requirements due to Oracle
# see also: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#installing-cx-oracle-on-linux
RUN mkdir -p /opt/oracle && \
    apt-get install -y --no-install-recommends libaio1 wget unzip && \
    cd /opt/oracle && \
    wget https://download.oracle.com/otn_software/linux/instantclient/19800/instantclient-basic-linux.x64-19.8.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.x64-19.8.0.0.0dbru.zip && \
    ldconfig /opt/oracle/instantclient_19_8 && \
    chmod -R 777 /opt/oracle

USER airflow

# required to run git with SSH
RUN mkdir -p /home/airflow/.ssh

# install psycopg2 - optional, but increases iteration speed
RUN pip install --user --upgrade psycopg2
