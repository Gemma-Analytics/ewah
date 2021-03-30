from ewah.hooks.base import EWAHBaseHook

from tempfile import NamedTemporaryFile
from typing import Tuple, Optional, Dict, Any

import sshtunnel
import os

# The default timeout of 10s is too little if airflow runs many EL tasks in
# parallel -> if the SSH connection attempts are done in parallel, the timeout
# hits before the connection is established.
sshtunnel.TUNNEL_TIMEOUT = 30


class EWAHSSHHook(EWAHBaseHook):

    _ATTR_RELABEL = {
        "username": "login",
    }

    conn_name_attr = "ewah_ssh_conn_id"
    default_conn_name = "ewah_ssh_default"
    conn_type = "ewah_ssh"
    hook_name = "EWAH SSH Connection"

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from wtforms import StringField
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from ewah.ewah_utils.widgets import EWAHTextAreaWidget

        return {
            "extra__ewah_ssh__private_key": StringField(
                "Private SSH Key (RSA Format)",
                widget=EWAHTextAreaWidget(rows=12),
            ),
            "extra__ewah_ssh__ssh_proxy_server": StringField(
                "SSH Proxy Server (optional)",
                widget=BS3TextFieldWidget(),
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {
                "host": "SSH Host",
                "port": "SSH Port (defaults to 22)",
                "login": "SSH Username",
                "password": "SSH Password OR Private Key Password",
            },
        }

    def start_tunnel(
        self, remote_host: str, remote_port: int, tunnel_timeout: Optional[int] = 30
    ) -> Tuple[str, int]:
        """Starts the SSH tunnel with port forwarding. Returns the host and port tuple
        that can be used to connect to the remote.

        :param remote_host: Host of the remote that the tunnel should port forward to.
            Can be "localhost" e.g. if tunneling into a server that hosts a database.
        :param remote_port: Port that goes along with remote_host.
        :param tunnel_timeout: Optional timeout setting. Supply a higher number if the
            default (30s) is too low.
        :returns: Local bind address aka tuple of local_bind_host and local_bind_port.
            Calls to local_bind_host:local_bind_port will be forwarded to
            remote_host:remote_port via the SSH tunnel.
        """

        if not hasattr(self, "_ssh_tunnel_forwarder"):
            # Tunnel is not started yet - start it now!

            # Set a specific tunnel timeout if applicable
            if tunnel_timeout:
                old_timeout = sshtunnel.TUNNEL_TIMEOUT
                sshtunnel.TUNNEL_TIMEOUT = tunnel_timeout

            try:
                # Build kwargs dict for SSH Tunnel Forwarder
                if self.conn.ssh_proxy_server:
                    # Use the proxy SSH server as target
                    self._ssh_hook = EWAHBaseHook.get_hook_from_conn_id(
                        conn_id=self.conn.ssh_proxy_server,
                    )
                    kwargs = {
                        "ssh_address_or_host": self._ssh_hook.start_tunnel(
                            self.conn.host, self.conn.port or 22
                        ),
                        "remote_bind_address": (remote_host, remote_port),
                    }
                else:
                    kwargs = {
                        "ssh_address_or_host": (self.conn.host, self.conn.port or 22),
                        "remote_bind_address": (remote_host, remote_port),
                    }

                if self.conn.username:
                    kwargs["ssh_username"] = self.conn.username
                if self.conn.password:
                    kwargs["ssh_password"] = self.conn.password

                # Save private key in a temporary file, if applicable
                with NamedTemporaryFile() as keyfile:
                    if self.conn.private_key:
                        keyfile.write(self.conn.private_key.encode())
                        keyfile.flush()
                        kwargs["ssh_pkey"] = os.path.abspath(keyfile.name)
                    self.log.info(
                        "Opening SSH Tunnel to {0}:{1}...".format(
                            *kwargs["ssh_address_or_host"]
                        )
                    )
                    self._ssh_tunnel_forwarder = sshtunnel.SSHTunnelForwarder(**kwargs)
                    self._ssh_tunnel_forwarder.start()
            except:
                # Set package constant back to original setting, if applicable
                if tunnel_timeout:
                    sshtunnel.TUNNEL_TIMEOUT = old_timeout
                raise

        return ("localhost", self._ssh_tunnel_forwarder.local_bind_port)

    def stop_tunnel(self) -> None:
        """Close an open SSH tunnel, if it is indeed open."""
        if hasattr(self, "_ssh_tunnel_forwarder"):
            self.log.info(
                "Closing SSH tunnel to {0}!".format(
                    str(self._ssh_tunnel_forwarder._remote_binds)
                )
            )
            self._ssh_tunnel_forwarder.stop()
            del self._ssh_tunnel_forwarder
            if hasattr(self, "_ssh_hook"):
                self._ssh_hook.stop_tunnel()

    def __del__(self) -> None:
        self.stop_tunnel()
