FROM apache/airflow:2.0.0-python3.8 as dev_build

### --------------------------------------------- run as root => ##
USER root
RUN apt-get update

# required packages to install psycopg2 which is a dependency of ewah
RUN apt-get install -y --no-install-recommends postgresql-server-dev-all gcc

# create folder and give user airflow sufficient access rights
RUN mkdir /opt/ewah && \
    chmod -R 777 /opt/ewah

# required to use git
RUN apt-get install -y --no-install-recommends git

# install requirements due to Oracle
# see also: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#installing-cx-oracle-on-linux
RUN mkdir -p /opt/oracle && \
    apt-get install -y --no-install-recommends libaio1 wget unzip && \
    cd /opt/oracle && \
    wget https://download.oracle.com/otn_software/linux/instantclient/19800/instantclient-basic-linux.x64-19.8.0.0.0dbru.zip && \
    unzip instantclient-basic-linux.x64-19.8.0.0.0dbru.zip && \
    rm -r -f /opt/oracle/instantclient-basic-linux.x64-19.8.0.0.0dbru.zip && \
    ldconfig /opt/oracle/instantclient_19_8 && \
    chmod -R 777 /opt/oracle

# enable sudo
RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers && \
    chmod 0440 /etc/sudoers

# overwrite entrypoint
COPY --chown=airflow:root  docker/scripts/entrypoint_prod.sh /entrypoint
COPY --chown=airflow:root  docker/scripts/entrypoint_prod.py /entrypoint.py

USER airflow
### <= --------------------------------------------- run as root ##


RUN pip install --user --upgrade --no-cache-dir pip setuptools

# required to make Oracle work with airflow user
RUN sudo ldconfig /opt/oracle/instantclient_19_8

# required to use SSH
RUN mkdir -p /home/airflow/.ssh

# install psycopg2 - optional, but increases iteration speed
RUN pip install --user --upgrade --no-cache-dir psycopg2

# Force using environment variables to set Fernet Key
ENV AIRFLOW__CORE__FERNET_KEY='Hello, I am AIRFLOW__CORE__FERNET_KEY and I need to be set in production!'

# Force overwrite of this secret via ENV VAR to ensure it is set uniquely
ENV AIRFLOW__WEBSERVER__SECRET_KEY=''

# Let entrypoint know to install from bind-mounted volume
ENV EWAH_IMAGE_TYPE='DEV'
# Run support scripts on start-up
ENV EWAH_RUN_DEV_SUPPORT_SCRIPTS='1'

# FYI
ENV EWAH_AIRFLOW_CONNS_YAML_PATH='You can set me as a path to a non-standard airflow connections yml file!'

# Create a superuser for the airflow backend - overwrite as applicable
ENV AIRFLOW__WEBSERVER__AUTHENTICATE=True
ENV AIRFLOW__WEBSERVER__AUTH_BACKEND='airflow.contrib.auth.backends.password_auth'
ENV EWAH_AIRFLOW_USER_SET='1'
ENV EWAH_AIRFLOW_USER_USER='ewah'
ENV EWAH_AIRFLOW_USER_PASSWORD='ewah'
ENV EWAH_AIRFLOW_USER_FIRSTNAME='ewah'
ENV EWAH_AIRFLOW_USER_LASTNAME='ewah'
ENV EWAH_AIRFLOW_USER_EMAIL='ewah@gemmaanalytics.com'

################################################################################
## Set a number of environment variables as EWAH defaults, can be overwritten ##
################################################################################
ENV AIRFLOW_HOME=/opt/airflow

# Useful, often changed configurations
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__PARALLELISM=8
ENV AIRFLOW__CORE__DAG_CONCURRENCY=16
ENV AIRFLOW__WEBSERVER__BASE_URL="http://localhost:8080"

# Email related
ENV AIRFLOW__EMAIL__EMAIL_BACKEND=airflow.utils.email.send_email_smtp
ENV AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
ENV AIRFLOW__SMTP__SMTP_STARTTLS=True
ENV AIRFLOW__SMTP__SMTP_SSL=False
# ENV AIRFLOW__SMTP__SMTP_USER=
# ENV AIRFLOW__SMTP__SMTP_PASSWORD=
ENV AIRFLOW__SMTP__SMTP_PORT=587
# ENV AIRFLOW__SMTP__SMTP_MAILFROM=

# Logging
ENV AIRFLOW__LOGGING__REMOTE_LOGGING=False
# ENV AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID=
# ENV AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=
# ENV AIRFLOW__LOGGING__ENCRYPT_S3_LOGS=
# ENV AIRFLOW__LOGGING__LOGGING_LEVEL = INFO

# Other useful configurations
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
ENV AIRFLOW__API__AUTH_BACKEND=airflow.api.auth.backend.deny_all
ENV AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False
ENV AIRFLOW__WEBSERVER__RBAC=False
ENV AIRFLOW__CORE__SECURE_MODE=True
ENV AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT=120

# Related to $AIRFLOW_HOME
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags
ENV AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
ENV AIRFLOW__CORE__PLUGINS_FOLDER=/opt/airflow/plugins
ENV AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION=/opt/airflow/logs/dag_processor_manager/dag_processor_manager.log
ENV AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY=/opt/airflow/logs/scheduler

# Default value socket.getfqdn sometimes cannot resolve hostname and falls back to gethostname()
# If that happens, all tasks fail - just use gethostname() from the start instead
ENV AIRFLOW__CORE__HOSTNAME_CALLABLE="socket.gethostname"

###############################################################################
## Multi-Stage build: for the publishable EWAH image, install EWAH from pip  ##
###############################################################################
FROM dev_build as prod_build

# don't install from bind-mounted volume
ENV EWAH_IMAGE_TYPE='PROD'

# don't run support scripts as default
# Overwrite this ENV to '1' if you'd like to auto-upgrade the metadata db &
# auto-set a default admin UI user (use ENV vars to set the credentials, namely:
# EWAH_AIRFLOW_USER_USER, EWAH_AIRFLOW_USER_PASSWORD, EWAH_AIRFLOW_USER_EMAIL)
ENV EWAH_RUN_DEV_SUPPORT_SCRIPTS='0'

# install from pip
# if you change the version, remember to change it in ewah/__init__.py as well!
RUN pip install --user --upgrade --no-cache-dir ewah==0.3.3

# copy default EWAH files into the dags folder
COPY airflow/kubernetes_dags_folder /opt/airflow/dags
