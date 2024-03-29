from airflow import settings
from airflow.models import Connection

from ewah.utils.yml_loader import Loader, Dumper

import os
import yaml
import json


def commit_conns(filepath: str) -> None:
    # read a yaml with connections and commit that to airflow's metadata db
    conns = yaml.load(open(filepath, "r"), Loader=Loader)  # read YAML
    session = settings.Session()  # connect to the metadata db
    existing_conns = session.query(Connection)
    for conn in conns.get("connections"):
        if not conn.get("id"):
            print("\n\nERROR: Connection {0} has no id!\n\n".format(str(conn)))
            continue
        id = conn["id"]
        extra = conn.get("extra")
        if isinstance(conn.get("extra"), dict):
            extra = json.dumps(extra)
        try:
            airflow_conn = (
                session.query(Connection).filter(Connection.conn_id == id).one()
            )
            print("Connection {0} exists - update!".format(id))
            airflow_conn.conn_id = id
            airflow_conn.conn_type = conn.get("type")
            airflow_conn.host = conn.get("host")
            airflow_conn.schema = conn.get("schema")
            airflow_conn.login = conn.get("login")
            airflow_conn.password = conn.get("password")
            airflow_conn.port = conn.get("port")
            airflow_conn.extra = extra
        except:
            print("Connection {0} does not exist - add!".format(id))
            airflow_conn = Connection(
                conn_id=id,
                conn_type=conn.get("type"),
                host=conn.get("host"),
                schema=conn.get("schema"),
                login=conn.get("login"),
                password=conn.get("password"),
                port=conn.get("port"),
                extra=extra,
            )
        session.add(airflow_conn)
        print('Added or updated connection "{0}"'.format(id))
    session.commit()
    print("Committed new connections.")
    session.close()


env = os.environ
# if they exist, add connections from appropriate YAML files
search_filepaths = [  # make sure all secret conns overwrite non-secret conns
    "/opt/airflow/docker/airflow_connections.yml",
    "/opt/airflow/airflow_connections.yml",
    "/opt/airflow/docker/secret_airflow_connections.yml",
    "/opt/airflow/docker/secrets/airflow_connections.yml",
    "/opt/airflow/docker/secrets/secret_airflow_connections.yml",
    "/opt/airflow/secret_airflow_connections.yml",
    "/opt/airflow/secrets/airflow_connections.yml",
    "/opt/airflow/secrets/secret_airflow_connections.yml",
]
if env.get("EWAH_AIRFLOW_CONNS_YAML_PATH"):
    search_filepaths += [env["EWAH_AIRFLOW_CONNS_YAML_PATH"]]

print("\n\n")
for filepath in search_filepaths:
    if os.path.isfile(filepath):
        print("Adding connection from file {0}...".format(filepath))
        commit_conns(filepath)
    else:
        print("Not a valid filepath: {0}".format(filepath))
print("\n\n")
