from ewah.constants import EWAHConstants as EC
from ewah.operators.base import EWAHBaseOperator
from ewah.hooks.amazon_ads import EWAHAmazonAdsHook

from datetime import timedelta


class EWAHAmazonAdsOperator(EWAHBaseOperator):

    _NAMES = ["amazon_ads"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: False,
    }

    def __init__(
        self,
        ads_type,
        report_type,
        profile_id=None,  # Pulls all available profiles if none is given
        additional_params=None,
        *args,
        **kwargs,
    ):
        # TODO: make error more explicit
        # TODO: validate via hook
        assert ads_type in ("sp", "sb", "sd", "hsa")

        if isinstance(profile_id, int):
            profile_id = str(profile_id)
        _msg = "profile_id, if supplied, must be a string or integer!"
        assert profile_id is None or isinstance(profile_id, str), _msg

        # TODO: auto-generate primary key (combined date + id, probably)

        super().__init__(*args, **kwargs)

        self.profile_id = profile_id
        self.ads_type = ads_type
        self.report_type = report_type
        self.additional_params = additional_params

    def ewah_execute(self, context):
        # Note: Incremental only!
        request_date = self.data_from.date()
        if isinstance(self.profile_id, str):
            profiles = [{"profileId": self.profile_id}]
        elif self.profile_id is None:
            profiles = self.source_hook.get_profile_ids()
        else:
            raise Exception("This should not happen")

        while request_date <= self.data_until.date():
            self.log.info(f"Requesting data for {request_date.isoformat()}...")
            # Get data for each profile
            for profile in profiles:
                if profile.get("accountInfo", {}).get("type") == "agency":
                    continue
                profile_id = profile["profileId"]
                self.log.info(f"Requesting data for profile {profile_id}...")
                report_data = self.source_hook.get_report(
                    date=request_date,
                    profile_id=profile_id,
                    ads_type=self.ads_type,
                    report_type=self.report_type,
                    additional_params=self.additional_params,
                )
                for datum in report_data:
                    datum["_report_date"] = request_date.isoformat()
                    datum["_profile_id"] = profile_id
                    datum["_profile_metadata"] = profile
                self.upload_data(report_data)
            request_date += timedelta(days=1)
