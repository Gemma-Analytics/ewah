from ewah.hooks.base import EWAHBaseHook

import requests


class EWAHInfigoHook(EWAHBaseHook):
    _ATTR_RELABEL = {
        "api_token": "password",
        "base_url": "host",
    }

    conn_name_attr = "ewah_infigo_conn_id"
    default_conn_name = "ewah_infigo_default"
    conn_type = "ewah_infigo"
    hook_name = "EWAH Infigo Connection"

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "login"],
            "relabeling": {"password": "API Token", "host": "Infigo Base URL"},
        }

    def make_api_request(self, url):
        response = requests.get(
            url, headers={"Authorization": "Basic {0}".format(self.conn.api_token)}
        )
        assert response.status_code == 200, "Error - Response {0}: {1}".format(
            response.status_code, response.text
        )
        return response.json()

    def get_orders(self, base_url):
        orders = self.make_api_request(base_url + "Order/List")
        data = []
        while orders:
            order = self.make_api_request(
                base_url + "Order/Detail/" + str(orders.pop(0))
            )
            order["OrderLineItems"] = [
                self.make_api_request(base_url + "OrderlineItem/Get/" + str(line_item))
                for line_item in order.get("OrderLineItems", [])
            ]
            data.append(order)
        return data

    def get_customers(self, base_url):
        customers = self.make_api_request(base_url + "customer")
        data = []
        while customers:
            data.append(
                self.make_api_request(
                    base_url + "Customer/Get/" + str(customers.pop(0))
                )
            )
        return data

    def get_printlocations(self, base_url):
        return self.make_api_request(base_url + "PrintLocation")

    def get_data(self, resource):
        base_url = self.conn.base_url
        if not base_url.endswith("/"):
            base_url += "/"
        if not base_url.endswith("services/api/"):
            base_url += "services/api/"
        resource = resource.lower()

        if resource in ["orders", "orders"]:
            return self.get_orders(base_url)
        elif resource in ["customer", "customers"]:
            return self.get_customers(base_url)
        elif resource in ["printlocation", "printlocations"]:
            return self.get_printlocations(base_url)
        else:
            raise Exception(
                "Invalid resource! may only fetch orders, customers, or printlocations!"
            )
