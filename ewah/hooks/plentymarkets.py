from airflow.models import Variable
from airflow.configuration import conf

from ewah.hooks.base import EWAHBaseHook

import requests
import json
from datetime import datetime, timedelta
from selenium import webdriver
from cryptography.fernet import Fernet


class EWAHPlentyMarketsHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "username": "login",
        "url": "host",
    }

    conn_name_attr = "ewah_plentymarkets_conn_id"
    default_conn_name = "ewah_plentymarkets_default"
    conn_type = "ewah_plentymarkets"
    hook_name = "EWAH PlentyMarkets Connection"

    _INCREMENTAL_FIELDS = {
        "/rest/orders": ["updatedAtFrom", "updatedAtTo"],
        "/rest/orders/status-history": ["createdAtFrom", "createdAtTo"],
        "/rest/accounts/contacts": ["updatedAtAfter", "updatedAtBefore"],
    }

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra"],
            "relabeling": {
                "login": "Username",
                "password": "Password",
                "host": "URL (e.g. https://myplentyshop.com or https://plentymarkets-cloud-07.com/12345)",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from wtforms import BooleanField

        return {
            "extra__ewah_plentymarkets__url_is_final": BooleanField(
                "Is this the final endpoint?"
            )
        }

    @property
    def endpoint(self):
        if self.conn.url_is_final:
            return self.conn.url

        # get the current endpoint with the correct backend hash
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1420,1080")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(self.conn.url)
        driver.find_element_by_id("username").send_keys(self.conn.username)
        driver.find_element_by_id("password").send_keys(self.conn.password)
        driver.find_element_by_xpath('//button[normalize-space()="Login"]').click()
        endpoint = driver.current_url
        if endpoint.count("/") > 2:
            endpoint = endpoint[
                : endpoint.find("/", endpoint.find("/", endpoint.find("/") + 1) + 1)
            ]
        return endpoint

    @property
    def token(self):
        if not hasattr(self, "_token") or datetime.now() > self._token_expires_at:
            # attempt to get the token from the Variable (if stored there)
            # Variable is encrypted with airflow instance's own fernet key
            # In the rare event of a key change, just load a new token
            # If currently rotating keys, use the first one
            fernet = Fernet(conf.get("core", "fernet_key").split(".")[0])
            ewah_plenty_token_variable_key = "__ewah_plenty_token"
            try:
                variable_decrypted = json.loads(
                    fernet.decrypt(
                        Variable.get(
                            key=ewah_plenty_token_variable_key, default_var=None
                        ).encode()
                    ).decode()
                )
            except:
                variable_decrypted = None

            if (
                variable_decrypted
                and variable_decrypted.get("expires_at")
                and datetime.fromisoformat(variable_decrypted["expires_at"])
                > datetime.utcnow()
            ):
                self.log.info("Using token stored as airflow variable")
                self._token = variable_decrypted["access_token"]
                self._token_expires_at = datetime.fromisoformat(
                    variable_decrypted["expires_at"]
                )
            else:
                self.log.info("Requesting new token")
                requested_at = datetime.now()
                token_request = requests.post(
                    self.endpoint + "/rest/login",
                    params={
                        "username": self.conn.username,
                        "password": self.conn.password,
                    },
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                )
                assert token_request.status_code == 200, token_request.text
                try:
                    request_data = token_request.json()
                except:
                    assert False, "Response is not a JSON - Response Text: {0}".format(
                        token_request.text
                    )
                self._token_expires_at = requested_at + timedelta(
                    seconds=request_data["expires_in"]
                )
                self._token = request_data["access_token"]
                Variable.set(
                    key=ewah_plenty_token_variable_key,
                    value=fernet.encrypt(
                        json.dumps(
                            {
                                "expires_at": self._token_expires_at.isoformat(),
                                "access_token": self._token,
                            }
                        ).encode()
                    ).decode(),
                )
        return self._token

    @staticmethod
    def format_resource(resource):
        if "rest/" in resource:
            if not resource.startswith("/"):
                resource = "/" + resource
        else:
            resource = "/rest/{0}".format(resource)
        return resource

    def get_data_in_batches(
        self,
        resource,
        data_from=None,
        data_until=None,
        batch_size=10000,
        additional_params=None,
    ):
        params = {
            "itemsPerPage": 250,  # Maximum
        }
        if additional_params:
            assert isinstance(additional_params, dict)
            params.update(additional_params)

        resource = self.format_resource(resource)
        url = self.endpoint + resource

        if data_from:
            params[self._INCREMENTAL_FIELDS[resource][0]] = data_from.isoformat()
        if data_until:
            if resource == "/rest/accounts/contacts":
                # inconsistent API implementation - ignores data for last day otherwise
                data_until += timedelta(days=1)
            params[self._INCREMENTAL_FIELDS[resource][1]] = data_until.isoformat()

        data = []
        while True:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": "Bearer {0}".format(self.token),
            }
            self.log.info("Requesting new page of data...")
            data_request = requests.get(url, params=params, headers=headers)
            assert data_request.status_code in (200, 401), "Status {0}: {1}".format(
                data_request.status_code, data_request.text
            )
            if data_request.status_code == 401:
                # Sometimes, the backend hash changes. If this happens during
                # a data load, is will result in a specific 401 error. Catch
                # the error and try again (once) with the new endpoint. The
                # try is needed in case the data_request() does not return
                # a JSON (will produce an error otherwise).
                try:
                    if data_request.json()["class"] == "UIHashExpiredException":
                        if "rest/" in resource:
                            url = self.endpoint + resource
                        else:
                            url = self.endpoint + "/rest/{0}".format(resource)
                        data_request = requests.get(url, params=params, headers=headers)
                except:
                    pass  # assert below will take care of any error
            assert data_request.status_code == 200, "Status {0}: {1}".format(
                data_request.status_code, data_request.text
            )
            returned_data = data_request.json()
            if isinstance(returned_data, list):
                # some endpoints return just one page with the data as list
                data += returned_data
                break
            else:
                if "entries" in returned_data.keys():
                    data += returned_data["entries"]
                else:
                    # Special case where just a dict with key-value pairs is returned
                    # Example: /rest/accounts/contacts/classes
                    data += [{"id": k, "value": v} for k, v in returned_data.items()]
                    break
                params["page"] = returned_data["page"] + 1  # for next request
                if returned_data["isLastPage"]:
                    break
            if len(data) >= batch_size:
                yield data
                data = []

        if data:
            yield data
