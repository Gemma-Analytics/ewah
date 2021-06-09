from ewah.hooks.base import EWAHBaseHook

import requests
from datetime import datetime, timedelta
from selenium import webdriver


class EWAHPlentyMarketsHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "username": "login",
        "url": "host",
    }

    conn_name_attr = "plentymarkets_conn_id"
    default_conn_name = "plentymarkets_default"
    conn_type = "ewah_plentymarkets"
    hook_name = "EWAH PlentyMarkets Connection"

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

    @property
    def endpoint(self):
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
            requested_at = datetime.now()
            token_request = requests.post(
                self.endpoint + "/rest/login",
                params={"username": self.conn.username, "password": self.conn.password},
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
        return self._token

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
        if data_from:
            params["updatedAtFrom"] = data_from.isoformat()
        if data_until:
            params["updatedAtTo"] = data_until.isoformat()
        if "rest/" in resource:
            if not resource.startswith("/"):
                resource = "/" + resource
            url = self.endpoint + resource
        else:
            url = self.endpoint + "/rest/{0}".format(resource)

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
            data += returned_data["entries"]
            if returned_data["isLastPage"]:
                break
            if len(data) >= batch_size:
                yield data
                data = []
            params["page"] = returned_data["page"] + 1  # for next request
        if data:
            yield data
