from ewah.constants import EWAHConstants as EC
from ewah.hooks.plentymarkets import EWAHPlentyMarketsHook
from ewah.operators.base import EWAHBaseOperator

"""temporary notes

- use selenium with phantomjs (https://phantomjs.org/download.html)

- find button via normalize space: https://stackoverflow.com/questions/49906237/how-to-find-button-with-selenium-by-its-text-inside-python

d = webdriver.PhantomJS("c:/phantomjs-2.1.1-windows/bin/phantomjs.exe")
d.set_window_size(1120, 550)
d.get("https://plentymarkets-cloud-07.com/50366")
d.find_element_by_id("username").send_keys("ga_bijan.soltani")
d.find_element_by_id("password").send_keys("jeak6KOSH6pird!waff")
d.find_element_by_xpath('//button[normalize-space()="Login"]').click()
d.current_url
"""


class EWAHPlentyMarketsOperator(EWAHBaseOperator):

    _NAMES = ["plentymarkets"]

    _ACCEPTED_EXTRACT_STRATEGIES = {
        EC.ES_FULL_REFRESH: True,
        EC.ES_INCREMENTAL: True,
        EC.ES_SUBSEQUENT: True,
    }

    _CONN_TYPE = EWAHPlentyMarketsHook.conn_type

    _REQUIRES_COLUMNS_DEFINITION = False

    def __init__(self, resource=None, additional_api_call_params=None, *args, **kwargs):
        kwargs["primary_key_column_name"] = "id"
        resource = resource or kwargs.get("target_table_name")
        if kwargs["extract_strategy"] == EC.ES_SUBSEQUENT:
            kwargs["subsequent_field"] = kwargs.get("subsequent_field", "updatedAt")
            # currently, only the orders resource works with subsequent loading
            assert resource == "orders"
        if kwargs["extract_strategy"] == EC.ES_INCREMENTAL:
            # currently, only the orders resource works with incremental loading
            assert resource == "orders"
        super().__init__(*args, **kwargs)

        assert isinstance(additional_api_call_params, (type(None), dict))
        self.resource = resource
        self.additional_api_call_params = additional_api_call_params

    def ewah_execute(self, context):
        if (
            self.extract_strategy == EC.ES_SUBSEQUENT
            and self.test_if_target_table_exists()
        ):
            data_from = self.get_max_value_of_column(self.subsequent_field)
        else:
            data_from = self.data_from
        for batch in self.source_hook.get_data_in_batches(
            resource=self.resource,
            data_from=data_from,
            data_until=self.data_until,
            additional_params=self.additional_api_call_params,
        ):
            self.upload_data(batch)
