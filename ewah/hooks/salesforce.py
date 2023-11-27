from ewah.hooks.base import EWAHBaseHook

from collections import OrderedDict
from simple_salesforce import Salesforce, format_soql
from typing import Union, List, Dict, Optional, Any
from datetime import datetime

import requests


class EWAHSalesforceHook(EWAHBaseHook):
    _ATTR_RELABEL: dict = {
        "username": "login",
        "domain": "schema",
    }

    conn_name_attr: str = "ewah_salesforce_conn_id"
    default_conn_name: str = "ewah_salesforce_default"
    conn_type: str = "ewah_salesforce"
    hook_name: str = "EWAH Salesforce Connection"

    _FIELDS_CACHE = {}

    _OAUTH_URL = "https://{0}.salesforce.com/services/oauth2/token"

    version = "49.0"

    @property
    def sf_conn(self) -> Salesforce:
        """Return an initialized Salesforce object based on the airflow connection."""

        if not hasattr(self, "_sf_conn"):
            self.log.info(
                f"Initializing connection with username {self.conn.username}!"
            )
            if self.conn.client_secret:
                response = requests.post(
                    self._OAUTH_URL.format(self.conn.domain or "login"),
                    data={
                        "client_secret": self.conn.client_secret,
                        "client_id": self.conn.client_id,
                        "grant_type": "password",
                        "username": self.conn.username,
                        "password": self.conn.password
                        + (self.conn.security_token or ""),
                    },
                ).json()
                self._sf_conn = Salesforce(
                    instance_url=response["instance_url"],
                    session_id=response["access_token"],
                    version=self.version,
                )
            else:
                self._sf_conn = Salesforce(
                    username=self.conn.username,
                    password=self.conn.password,
                    security_token=self.conn.security_token,
                    domain=self.conn.domain or None,
                    client_id=self.conn.client_id or None,
                    version=self.version,
                )
        return self._sf_conn

    def get_incrementer(self, salesforce_object):
        # Figure out what field to use for date comparison
        fields = self.get_fields_of_object(salesforce_object)
        options = ["SystemModstamp", "LastModifiedDate", "CreatedDate"]
        for option in options:
            if option in fields:
                return option

    def get_data_in_batches(
        self,
        salesforce_object: str,
        columns: Optional[list] = None,
        data_from: Optional[datetime] = None,
        data_until: Optional[datetime] = None,
        batch_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """Generator to return all data of a salesforce object"""

        # Figure out what field(s) to use for date comparison & data loading
        fields = self.get_fields_of_object(salesforce_object)
        incrementer = self.get_incrementer(salesforce_object=salesforce_object)

        # Create SOQL query
        query = "SELECT\n\t{0}\nFROM {1}".format(
            ",\n\t".join(columns or fields), salesforce_object
        )
        where_clauses = []
        if data_from and incrementer:
            where_clauses.append(f"{incrementer} >= " + format_soql("{0}", data_from))
        if data_until and incrementer:
            where_clauses.append(f"{incrementer} < " + format_soql("{0}", data_until))
        if where_clauses:
            query += "\nWHERE " + "\n  AND ".join(where_clauses)
        self.log.info(f"Querying data with this SOQL query:\n\n{query}")

        # Yield results
        result = self.sf_conn.query(query, include_deleted=True)
        data = []
        while (result.get("done") and result.get("records")) or result.get(
            "nextRecordsUrl"
        ):
            result_data = result.pop("records")
            for datum in result_data:
                del datum["attributes"]
                datum = dict(datum)
            data += result_data
            next_page = result.get("nextRecordsUrl")
            if next_page:
                result = self.sf_conn.query_more(next_page, True)
            if len(data) >= batch_size:
                yield data
                data = []

        if data:
            yield data

        # Make sure there was no error on the way
        if not result.get("done"):
            raise Exception("SOQL query error! Response: {0}".format(result))

    def get_soql_query_result(
        self, soql_query: str, remove_attribues: bool = True, convert_dict: bool = False
    ) -> Union[List[OrderedDict], List[dict]]:
        """Returns the result of an SOQL query.

        Avoid running multiple large queries simultaneously. Queries are paginated and
        Salesforce will only ever make the last 10 pages available. If running too many
        queries at once, this may mean that a page will become unavailable before it
        was fetched, failing the task.

        :param soql_query: SOQL query to run and return result.
        :param remove_attribues: If TRUE, removes "attributes" field from data
            dictionary.
        :param convert_dict: If TRUE, converts result in a list of normal dictionaries.
            Otherwise, will return a list of OrderedDict.

        """
        result = self.sf_conn.query_all(soql_query)

        if not result.get("done"):
            raise Exception("SOQL query error! Response: {0}".format(result))

        data = result["records"]

        if data and (remove_attribues or convert_dict):
            for datum in data:
                if remove_attribues:
                    del datum["attributes"]
                if convert_dict:
                    datum = dict(datum)

        return data

    def get_fields_of_object(
        self, salesforce_object: str, return_types: bool = False
    ) -> Union[List[str], Dict[str, str]]:
        """Returns a list or dict of all fields of a Salesforce object.

        :param salesforce_object: Name of the Salesforce object (e.g. Account)
        :param return_types: If TRUE, return a dict of field_name:field_type; if FALSE,
            return a list of field names.
        """
        if self._FIELDS_CACHE.get(salesforce_object):
            fields = self._FIELDS_CACHE[salesforce_object]
        else:
            fields = {
                field["name"]: field["type"]
                for field in getattr(self.sf_conn, salesforce_object).describe()[
                    "fields"
                ]
            }
            self._FIELDS_CACHE[salesforce_object] = fields

        if return_types:
            return fields
        else:
            return [key for (key, value) in fields.items()]

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["port", "extra", "host"],
            "relabeling": {
                "login": "Username",
                "password": "Password",
                "schema": "Domain (default: 'login')",
            },
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from wtforms import StringField

        return {
            "extra__ewah_salesforce__security_token": StringField(
                "Security Token",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_salesforce__client_id": StringField(
                "Client ID (optional)",
                widget=BS3TextFieldWidget(),
            ),
            "extra__ewah_salesforce__client_secret": StringField(
                "Client Secret (optional)",
                widget=BS3TextFieldWidget(),
            ),
        }
