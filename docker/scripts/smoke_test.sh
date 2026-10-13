#!/usr/bin/env bash
# Smoke test for a built EWAH image. Catches dependency combinations that pip
# accepts but that break at import time (e.g. pyOpenSSL 22 with cryptography 39).
# Needs no network access - run it with `--network none`:
#   docker run --rm --network none --entrypoint bash \
#     -v "$PWD/docker/scripts/smoke_test.sh:/smoke_test.sh:ro" <image> /smoke_test.sh
set -euo pipefail

export AIRFLOW_HOME=/tmp/smoke_airflow
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN="sqlite:///${AIRFLOW_HOME}/airflow.db"
export AIRFLOW__CORE__FERNET_KEY='kiQHALe31o7by-d9U-lzxhpDsmllTmu0DUagDuYQoWs='
export AIRFLOW__WEBSERVER__SECRET_KEY='smoke-test'
export AIRFLOW__CORE__DAGS_FOLDER="${AIRFLOW_HOME}/dags"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="${AIRFLOW_HOME}/logs"
export AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION="${AIRFLOW_HOME}/logs/dpm.log"
export AIRFLOW__SCHEDULER__CHILD_PROCESS_LOG_DIRECTORY="${AIRFLOW_HOME}/logs/scheduler"
mkdir -p "${AIRFLOW__CORE__DAGS_FOLDER}"

echo "== Versions"
pip list 2>/dev/null | grep -i -E "^(pyopenssl|cryptography|snowflake-connector-python|botocore|ewah) "

echo "== Imports"
python -c "import OpenSSL, botocore, snowflake.connector, ewah; print('imports ok')"

echo "== airflow db init"
airflow db init > "${AIRFLOW_HOME}/db_init.log" 2>&1 \
  || { cat "${AIRFLOW_HOME}/db_init.log"; exit 1; }

echo "== Webserver /health"
airflow webserver --port 8080 > "${AIRFLOW_HOME}/webserver.log" 2>&1 &
for _ in $(seq 1 60); do
  if curl -sf -o /dev/null http://localhost:8080/health; then
    echo "webserver /health 200"
    exit 0
  fi
  sleep 5
done
cat "${AIRFLOW_HOME}/webserver.log"
echo "webserver did not become healthy" >&2
exit 1
