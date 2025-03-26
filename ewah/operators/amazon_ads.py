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
        api_version="v2",  # default as long as not all endpoints migrated
        *args,
        **kwargs,
    ):
        # TODO: make error more explicit
        # TODO: validate via hook
        assert (ads_type in ("sp", "sb", "sd", "hsa") and api_version == "v2") or (
            ads_type in ("SPONSORED_PRODUCTS", "SPONSORED_BRANDS", "SPONSORED_DISPLAY") and api_version == "v3"
        ), "ads_type and api_version combination not supported"

        assert api_version in ("v2", "v3"), "only Amazon Ads v2 or v3 supported"

        if isinstance(profile_id, int):
            profile_id = str(profile_id)
        _msg = "profile_id, if supplied, must be a string or integer!"
        assert profile_id is None or isinstance(profile_id, str), _msg

        # TODO: auto-generate primary key (combined date + id, probably)

        super().__init__(*args, **kwargs)

        self.profile_id = profile_id
        self.ads_type = ads_type
        self.report_type = report_type
        self.api_version = api_version
        self.additional_params = additional_params

    def ewah_execute(self, context):
        # Note: Incremental only!
        start_date = self.data_from.date()
        end_date = self.data_until.date()
        if isinstance(self.profile_id, str):
            profiles = [{"profileId": self.profile_id}]
        elif self.profile_id is None:
            profiles = self.source_hook.get_profile_ids()
        else:
            raise Exception("This should not happen")

        if self.api_version == "v2":
            while start_date <= end_date:
                self.log.info(f"Requesting data for {start_date.isoformat()}...")
                # Get data for each profile
                for profile in profiles:
                    if profile.get("accountInfo", {}).get("type") == "agency":
                        continue
                    profile_id = profile["profileId"]
                    self.log.info(f"Requesting data for profile {profile_id}...")
                    
                    report_data = self.source_hook.get_report(
                        date=start_date,
                        profile_id=profile_id,
                        ads_type=self.ads_type,
                        report_type=self.report_type,
                        additional_params=self.additional_params,
                    )

                    # Enrich all records with metadata
                    for datum in report_data:
                        datum["_report_date"] = start_date.isoformat()
                        datum["_profile_id"] = profile_id
                        datum["_profile_metadata"] = profile
                    
                    # Upload once after enriching all records
                    self.upload_data(report_data)
                start_date += timedelta(days=1)
                
        if self.api_version == "v3":
            # Get data for each profile
            for profile in profiles:
                if profile.get("accountInfo", {}).get("type") == "agency":
                    continue
                profile_id = profile["profileId"]
                self.log.info(f"Requesting data for profile {profile_id}...")
                self.log.info(f"Start date: {start_date.isoformat()}")
                self.log.info(f"End date: {end_date.isoformat()}")
                
                report_data = self.source_hook.get_report_v3(
                    start_date=start_date,
                    end_date=end_date,
                    profile_id=profile_id,
                    ad_product=self.ads_type,
                    report_type=self.report_type,
                    additional_params=self.additional_params,
                )

                # Enrich all records with metadata
                for datum in report_data:
                    datum["_report_date"] = end_date.isoformat()
                    datum["_profile_id"] = profile_id
                    datum["_profile_metadata"] = profile
                
                # Upload once after enriching all records
                self.upload_data(report_data)



class EWAHAmazonAdsDSPOperator(EWAHBaseOperator):
    # Specifically for DSP endpoints

    _NAMES = ["amazon_ads_dsp"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: False,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: False,
    }

    def __init__(
        self,
        report_type,
        account_id,
        metrics=None,
        dimensions=None,
        request_period_days=30,
        *args,
        **kwargs,
    ):
        # TODO: auto-generate primary key (combined date + id, probably)

        super().__init__(*args, **kwargs)

        self.account_id = account_id
        self.report_type = report_type
        self.metrics = metrics
        self.dimensions = dimensions
        self.request_period_days = request_period_days

    def ewah_execute(self, context):
        # Note: Incremental only!
        request_date_start = self.data_from.date()
        request_date_end = min(
            self.data_until.date(),
            request_date_start + timedelta(days=self.request_period_days),
        )

        while True:
            # Maximum 30 days range per request
            self.log.info(
                f"Requesting data from {request_date_start.isoformat()} "
                f"until {request_date_end.isoformat()}..."
            )
            report_data = self.source_hook.get_report_dsp(
                account_id=self.account_id,
                report_date_start=request_date_start.isoformat(),
                report_date_end=request_date_end.isoformat(),
                report_type=self.report_type,
                metrics=self.metrics,
                dimensions=self.dimensions,
            )
            self.upload_data(report_data)
            if request_date_end == self.data_until.date():
                break
            else:
                request_date_start = request_date_end + timedelta(days=1)
                request_date_end = min(
                    self.data_until.date(),
                    request_date_start + timedelta(days=self.request_period_days),
                )
