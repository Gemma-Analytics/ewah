from ewah.hooks.base import EWAHBaseHook


class EWAHSSHHook(EWAHBaseHook):

    conn_name_attr = "ssh_conn_id"
    default_conn_name = "ssh_default"
    conn_type = "ewah_ssh"
    hook_name = "EWAH SSH Connection"

    @staticmethod
    def get_connection_form_widgets():
        """Returns connection widgets to add to connection form"""
        from wtforms import StringField
        from ewah.ewah_utils.widgets import EWAHTextAreaWidget

        return {
            "extra__ewah_ssh__private_key": StringField(
                "Private SSH Key (RSA Format)",
                widget=EWAHTextAreaWidget(rows=12),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour():
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {
                "host": "SSH Host",
                "port": "SSH Port (defaults to 22)",
                "login": "SSH Username",
                "password": "SSH Password OR Private Key Password",
            },
        }
