from ewah.constants import EWAHConstants as EC
from ewah.hooks.base import EWAHBaseHook

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials as SAC

import json
import time
from datetime import datetime, timedelta


class EWAHGoogleAnalyticsHook(EWAHBaseHook):

    _ATTR_RELABEL = {}

    conn_name_attr = "ewah_google_analytics_conn_id"
    default_conn_name = "ewah_google_analytics_default"
    conn_type = "ewah_google_analytics"
    hook_name = "EWAH Google Analytics Connection"

    METRICMAP = {
        "METRIC_TYPE_UNSPECIFIED": "varchar(255)",
        "CURRENCY": "decimal(20,5)",
        "INTEGER": "int(11)",
        "FLOAT": "decimal(20,5)",
        "PERCENT": "decimal(20,5)",
        "TIME": "time",
    }

    _SAMPLE_JSON = {
        "client_secrets": {
            "type": "service_account",
            "project_id": "abc-123",
            "private_key_id": "123456abcder",
            "private_key": "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----\n",
            "client_email": "xyz@abc-123.iam.gserviceaccount.com",
            "client_id": "123457",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xyz%40abc-123.iam.gserviceaccount.com",
        }
    }

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "password", "login", "schema", "host", "port"],
            "relabeling": {},
        }

    @staticmethod
    def get_connection_form_widgets() -> dict:
        """Returns connection widgets to add to connection form"""
        # from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from ewah.ewah_utils.widgets import EWAHTextAreaWidget
        from wtforms import StringField

        return {
            "extra__ewah_google_analytics__service_account_json": StringField(
                "Service Account JSON",
                widget=EWAHTextAreaWidget(rows=12),
            ),
        }

    def get_data_in_batches(self, data_from, data_until, chunking_interval, **kwargs):
        while data_from <= data_until:
            yield self.get_data(
                data_from=data_from,
                data_until=min(data_until, data_from + chunking_interval),
                **kwargs
            )
            data_from += chunking_interval + timedelta(days=1)

    def get_data(
        self,
        view_id,
        dimensions,
        metrics,
        page_size,
        include_empty_rows,
        sampling_level,
        data_from,
        data_until,
    ):
        sac_json = self.conn.service_account_json
        try:
            sac_json = json.loads(sac_json)
        except:
            raise Exception("Service Account JSON is not valid JSON!")

        if sac_json.get("client_secrets"):
            sac_json = sac_json.get("client_secrets")

        if not all(
            [key in sac_json.keys() for key in self._SAMPLE_JSON["client_secrets"]]
        ) and len(sac_json.keys()) == len(self._SAMPLE_JSON["client_secrets"]):
            _msg = "Google Analytics Credentials misspecified!"
            _msg += " Example of a correct Service Account JSON: {0}".format(
                json.dumps(self._SAMPLE_JSON)
            )
            raise Exception(_msg)

        if len(dimensions) > 7:
            raise Exception(
                (
                    "Can only fetch up to 7 dimensions!" + " Currently {0} Dimensions"
                ).format(
                    str(len(dimensions)),
                )
            )

        if len(metrics) > 10:
            raise Exception(
                (
                    "Can only fetch up to 10 metrics!" + " Currently {0} Dimensions"
                ).format(
                    str(len(metrics)),
                )
            )

        if page_size > 10000:
            raise Exception("Please specify a page size equal to or lower than 10000.")

        service_object = build(
            "analyticsreporting",
            "v4",
            credentials=SAC.from_json_keyfile_dict(
                sac_json,
                ["https://www.googleapis.com/auth/analytics.readonly"],
            ),
        )

        report_request = {
            "viewId": str(view_id),
            "dateRanges": [
                {
                    "startDate": data_from.isoformat(),
                    "endDate": data_until.isoformat(),
                }
            ],
            "samplingLevel": sampling_level,
            "dimensions": [{"name": d} for d in dimensions],
            "metrics": [{"expression": m} for m in metrics],
            "pageSize": page_size,
            "includeEmptyRows": include_empty_rows,
        }

        self.log.info(
            "Loading data from {0} to {1}...".format(
                report_request["dateRanges"][0]["startDate"],
                report_request["dateRanges"][0]["endDate"],
            )
        )

        report_response = (
            service_object.reports()
            .batchGet(body={"reportRequests": [report_request]})
            .execute()
        )

        if report_response.get("reports"):
            report = report_response["reports"][0]
            rows = report.get("data", {}).get("rows", [])

            while report.get("nextPageToken"):
                time.sleep(1)
                report_request.update({"pageToken": report["nextPageToken"]})
                report_response = (
                    service_object.reports()
                    .batchGet(body={"reportRequests": [report_request]})
                    .execute()
                )
                report = report_response["reports"][0]
                rows.extend(report.get("data", {}).get("rows", []))

            if report["data"]:
                report["data"]["rows"] = rows
        else:
            report = {}

        column_header = report.get("columnHeader", {})
        # Right now all dimensions are hardcoded to varchar(255), will need a
        # map if any non-varchar dimensions are used in the future
        # Unfortunately the API does not send back types for Dimensions like it
        # does for Metrics (yet..)
        dimension_headers = [
            {
                "name": header.replace("ga:", ""),
                "type": "varchar(255)",
            }
            for header in column_header.get("dimensions", [])
        ]
        metric_headers = [
            {
                "name": entry.get("name").replace("ga:", ""),
                "type": self.METRICMAP.get(entry.get("type"), "varchar(255)"),
            }
            for entry in column_header.get("metricHeader", {}).get(
                "metricHeaderEntries", []
            )
        ]

        self.log.info("Reading data from fetched report...")
        uploadable_data = []
        rows = report.get("data", {}).get("rows", [])
        for row_counter, row in enumerate(rows):
            root_data_obj = {}
            dimensions = row.get("dimensions", [])
            metrics = row.get("metrics", [])

            for index, dimension in enumerate(dimensions):
                header = dimension_headers[index].get("name")  # .lower()
                root_data_obj[header] = dimension

            for metric in metrics:
                data = {}
                data.update(root_data_obj)

                for index, value in enumerate(metric.get("values", [])):
                    header = metric_headers[index].get("name")  # .lower()
                    data[header] = value

                data["view_id"] = view_id
                if data.get("date"):
                    data["date"] = datetime.strptime(data["date"], "%Y%m%d").date()
                if data.get("dateHour"):
                    data["dateHour"] = datetime.strptime(data["dateHour"], "%Y%m%d%H")

                uploadable_data += [data]

        return uploadable_data
