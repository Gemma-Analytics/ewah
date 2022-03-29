from ewah.constants import EWAHConstants as EC
from ewah.hooks.base import EWAHBaseHook

from datetime import datetime, date
from typing import Optional, Dict, List

import json
import requests


class EWAHLinkedInHook(EWAHBaseHook):

    _ATTR_RELABEL: {}

    conn_name_attr = "ewah_linkedin_conn_id"
    default_conn_name = "ewah_linkedin_default"
    conn_type = "ewah_linkedin"
    hook_name = "EWAH LinkedIn Connection"

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        return {
            "hidden_fields": ["extra", "host", "port", "login", "schema"],
            "relabeling": {
                "password": "Access Token",
            },
        }

    _AVAILABLE_FIELDS_ANALYTICS = {
        "actionClicks",
        "adUnitClicks",
        "approximateUniqueImpressions",
        "cardClicks",
        "cardImpressions",
        "clicks",
        "commentLikes",
        "comments",
        "companyPageClicks",
        "conversionValueInLocalCurrency",
        "costInLocalCurrency",
        "costInUsd",
        "dateRange",
        "externalWebsiteConversions",
        "externalWebsitePostClickConversions",
        "externalWebsitePostViewConversions",
        "follows",
        "fullScreenPlays",
        "impressions",
        "landingPageClicks",
        "leadGenerationMailContactInfoShares",
        "leadGenerationMailInterestedClicks",
        "likes",
        "oneClickLeadFormOpens",
        "oneClickLeads",
        "opens",
        "otherEngagements",
        "pivot",
        "pivotValue",
        "pivotValues",
        "reactions",
        "sends",
        "shares",
        "textUrlClicks",
        "totalEngagements",
        "videoCompletions",
        "videoFirstQuartileCompletions",
        "videoMidpointCompletions",
        "videoStarts",
        "videoThirdQuartileCompletions",
        "videoViews",
        "viralCardClicks",
        "viralCardImpressions",
        "viralClicks",
        "viralCommentLikes",
        "viralComments",
        "viralCompanyPageClicks",
        "viralExternalWebsiteConversions",
        "viralExternalWebsitePostClickConversions",
        "viralExternalWebsitePostViewConversions",
        "viralFollows",
        "viralFullScreenPlays",
        "viralImpressions",
        "viralLandingPageClicks",
        "viralLikes",
        "viralOneClickLeadFormOpens",
        "viralOneClickLeads",
        "viralOtherEngagements",
        "viralReactions",
        "viralShares",
        "viralTotalEngagements",
        "viralVideoCompletions",
        "viralVideoFirstQuartileCompletions",
        "viralVideoMidpointCompletions",
        "viralVideoStarts",
        "viralVideoThirdQuartileCompletions",
        "viralVideoViews",
    }

    _REQUIRED_FIELDS_ANALYTICS = {
        "dateRange",
        "pivotValue",
    }

    _AVAILABLE_OBJECTS = {"Accounts", "Campaigns", "Creatives", "CampaignGroups"}

    @staticmethod
    def get_cleaner_callables():
        # dateRange madness is fixed here
        def cast_date_range(row):
            date_range = row.pop("dateRange", None)
            if not date_range:
                return row
            date_start = date_range.pop("start")
            date_end = date_range.pop("end")

            row["date_start"] = date(
                date_start["year"], date_start["month"], date_start["day"]
            )
            row["date_end"] = date(date_end["year"], date_end["month"], date_end["day"])

            return row

        return [cast_date_range]

    @classmethod
    def validate_fields_list(cls, fields_list: List[str]) -> bool:
        assert isinstance(fields_list, list), "Must supply a list of fields!"
        for field in cls._REQUIRED_FIELDS_ANALYTICS:
            assert field in fields_list, f"Must include the field {field}!"
        assert len(fields_list) <= 20, "Can only have a maximum of 20 fields!"
        for field in fields_list:
            assert (
                field in cls._AVAILABLE_FIELDS_ANALYTICS
            ), "Field {0} not part of allowed fields:\n\n{1}".format(
                field, "\n\t".join(cls._AVAILABLE_FIELDS_ANALYTICS)
            )
        return True

    @property
    def call_headers(self) -> Dict[str, str]:
        return {
            "Authorization": "Bearer {0}".format(self.conn.password),
            "Accept": "application/json",
        }

    def get_account_list(self, return_only_ids=True):
        return [account["id"] for account in self.get_object_data("Accounts")]

    def get_analytics_for_account(
        self, account_id, pivot, date_start, date_end, fields
    ):
        # Note that pagination is not supported for this endpoint
        self.validate_fields_list(fields)
        url = "https://api.linkedin.com/v2/adAnalyticsV2"
        self.log.info("Pulling analytics data for account {0}".format(account_id))
        params = {
            "q": "analytics",
            "pivot": pivot,
            "dateRange.start.day": date_start.day,
            "dateRange.start.month": date_start.month,
            "dateRange.start.year": date_start.year,
            "dateRange.end.day": date_end.day,
            "dateRange.end.month": date_end.month,
            "dateRange.end.year": date_end.year,
            "timeGranularity": "DAILY",
            "accounts[0]": "urn:li:sponsoredAccount:{0}".format(account_id),
            "fields": ",".join(fields),
        }

        response = requests.get(url, params=params, headers=self.call_headers)
        assert response.status_code == 200, response.text
        return response.json()["elements"]

    def get_analytics_in_batches(
        self, pivot, date_start, date_end, fields, account_ids=None
    ):
        if not account_ids:
            account_ids = self.get_account_list()

        for account_id in account_ids:
            yield self.get_analytics_for_account(
                account_id, pivot, date_start, date_end, fields
            )

    def get_object_data(self, object_name):
        url = "https://api.linkedin.com/v2/ad{0}V2".format(object_name)
        params = {
            "q": "search",
            "start": 0,
            "count": 100,  # 100 is the max value for count
        }
        data = []
        while True:
            response = requests.get(url, params=params, headers=self.call_headers)
            assert response.status_code == 200, response.text
            elements = response.json()["elements"]
            if elements:
                data += elements
                if len(elements) < params["count"]:
                    break
                params["start"] = params["start"] + params["count"]
            else:
                break
        return data
