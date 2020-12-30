from ewah.hooks.base import EWAHBaseHook


class EWAHAircallHook(EWAHBaseHook):

    conn_name_attr = "aircall_conn_id"
    default_conn_name = "aircall_default"
    conn_type = "ewah_aircall"
    hook_name = "EWAH Aircall Connection"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["port", "schema", "extra", "host"],
            "relabeling": {
                "login": "Basic Auth API ID",
                "password": "Baisc Auth API Token",
            },
        }

    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from wtforms import StringField

        return {
            "extra__ewah_aircall__random_text_field": StringField(
                "Delete me before commit!",
                widget=BS3TextFieldWidget(),
            ),
        }
