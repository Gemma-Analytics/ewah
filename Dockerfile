FROM apache/airflow as dev_build

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


RUN pip install --user --upgrade pip setuptools

# required to make Oracle work with airflow user
RUN sudo ldconfig /opt/oracle/instantclient_19_8

# required to use SSH
RUN mkdir -p /home/airflow/.ssh

# install psycopg2 - optional, but increases iteration speed
RUN pip install --user --upgrade psycopg2

# install flask-bcrypt to enable use of the backend
RUN pip install flask-bcrypt

# Force using environment variables to set Fernet Key & Metadata Database conn
ENV AIRFLOW__CORE__FERNET_KEY='Hello, I am AIRFLOW__CORE__FERNET_KEY and I need to be set in production!'
ENV AIRFLOW__CORE__SQL_ALCHEMY_CONN='Hello, I am AIRFLOW__CORE__SQL_ALCHEMY_CONN and I need to be set in production!'

# Let entrypoint know to install from bind-mounted volume
ENV EWAH_IMAGE_TYPE='DEV'

# FYI
ENV EWAH_AIRFLOW_CONNS_YAML_PATH='You can set me as a path to a non-standard airflow connections yml file!'

# Create a superuser for the airflow backend - overwrite as applicable
ENV AIRFLOW__WEBSERVER__AUTHENTICATE=True
ENV AIRFLOW__WEBSERVER__AUTH_BACKEND='airflow.contrib.auth.backends.password_auth'
ENV EWAH_AIRFLOW_USER_SET=True
ENV EWAH_AIRFLOW_USER_USER='ewah'
ENV EWAH_AIRFLOW_USER_PASSWORD='ewah'
ENV EWAH_AIRFLOW_USER_EMAIL='ewah@gemmaanalytics.com'

###############################################################################
## Multi-Stage build: for the publishable EWAH image, install EWAH from pip  ##
###############################################################################
FROM dev_build as prod_build

# don't install from bind-mounted volume
ENV EWAH_IMAGE_TYPE='PROD'

# install from pip
RUN pip install --user --upgrade ewah
