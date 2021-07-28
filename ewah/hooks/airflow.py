from ewah.hooks.base import EWAHBaseHook

import requests


class EWAHAirflowHook(EWAHBaseHook):
    """Get Airflow Metadata from an Airflow installation via the stable API."""

    _ATTR_RELABEL = {
        "url": "host",
        "user": "login",
    }

    conn_name_attr = "ewah_airflow_conn_id"
    default_conn_name = "ewah_airflow_default"
    conn_type = "ewah_airflow"
    hook_name = "EWAH Airflow Connection"

    # Resolve some more complex endpoints
    _ENDPOINTS = {
        "dagRuns": "dags/~/dagRuns",
        "taskInstance": "dags/~/dagRuns/~/taskInstances",
        "taskInstances": "dags/~/dagRuns/~/taskInstances",
    }

    _BASE_URL = "{0}/api/v1/{1}"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {
                "host": "URL (without endpoint, e.g. 'myairflowurl.com')",
                "login": "Basic Auth Username",
                "password": "Basic Auth Password",
                "port": "Port (only if using SSH)",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from wtforms import StringField

        return {
            "extra__ewah_airflow__ssh_conn_id": StringField(
                "SSH Connection ID to Airflow Server (optional)",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_airflow__protocol": StringField(
                # Note: must not use SelectField
                "Connection protocol (if using SSH or not specified in URL; one of: http, https)",
                default="http",
            ),
        }

    def get_data_in_batches(self, endpoint, page_size=100, batch_size=10000):
        auth = requests.auth.HTTPBasicAuth(self.conn.login, self.conn.password)
        if self.conn.ssh_conn_id:
            ssh_hook = EWAHBaseHook.get_hook_from_conn_id(conn_id=self.conn.ssh_conn_id)
            ssh_host = self.conn.host or "localhost"
            ssh_host = ssh_host.replace("https://", "").replace("http://", "")
            ssh_port = self.conn.port
            if not ssh_port:
                if self.conn.protocol == "http":
                    ssh_port = 80
                else:
                    ssh_port = 443
            else:
                ssh_port = int(ssh_port)
            local_bind_address = ssh_hook.start_tunnel(ssh_host, ssh_port)
            host = "{2}://{0}:{1}".format(
                local_bind_address[0],
                str(local_bind_address[1]),
                self.conn.protocol or "http",
            )
        else:
            host = self.conn.host
            if not host.startswith("http"):
                host = self.conn.protocol + "://" + host
        url = self._BASE_URL.format(host, self._ENDPOINTS.get(endpoint, endpoint))
        params = {"limit": page_size, "offset": 0}
        data = []
        i = 0
        while True:
            i += 1
            self.log.info("Making request {0} to {1}...".format(i, url))
            request = requests.get(url, params=params, auth=auth)
            assert request.status_code == 200, request.text
            response = request.json()
            keys = list(response.keys())
            if "total_entries" in keys:
                # Most endpoint use pagination + give "total_entries" for requests
                # The key to get the data from the response may differ from endpoint
                if keys[0] == "total_entries":
                    data_key = keys[1]
                else:
                    data_key = keys[0]
                if not response[data_key]:
                    # You know that you fetched all items if an empty list is returned
                    # (Note: total_entries is not reliable)
                    yield data
                    data = []
                    break
                data += response[data_key]
                if len(data) >= batch_size:
                    yield data
                    data = []
            else:
                # Rare endpoint that does not paginate (usually singletons)
                yield [response]
                break
            params["offset"] = params["offset"] + params["limit"]

        if self.conn.ssh_conn_id:
            ssh_hook.stop_tunnel()
            del ssh_hook
