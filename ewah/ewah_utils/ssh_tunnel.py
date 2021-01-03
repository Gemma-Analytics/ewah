from ewah.hooks.base import EWAHBaseHook

import os

from tempfile import NamedTemporaryFile

import sshtunnel

# The default timeout of 10s is too little if airflow runs many EL tasks in
# parallel -> if the SSH connection attempts are done in parallel, the timeout
# hits before the connection is established.
sshtunnel.TUNNEL_TIMEOUT = 30


def start_ssh_tunnel(ssh_conn_id, remote_conn_id, custom_timeout=None):
    """
    Start the SSH Tunnel with appropriate port forwarding.

    Returns both the SSHTunnelForwarder object for later closure and an
    adjusted connection object (host and port are adjusted in accordance with
    the open tunnel)
    """
    if custom_timeout:
        previous_timeout = sshtunnel.TUNNEL_TIMEOUT
        sshtunnel.TUNNEL_TIMEOUT = custom_timeout

    ssh_conn = EWAHBaseHook.get_connection(ssh_conn_id)
    remote_conn = EWAHBaseHook.get_connection(remote_conn_id)
    kwargs = {
        "ssh_address_or_host": (ssh_conn.host, ssh_conn.port or 22),
        "remote_bind_address": (remote_conn.host, remote_conn.port),
    }
    if ssh_conn.login:
        kwargs["ssh_username"] = ssh_conn.login or None
    if ssh_conn.password:
        kwargs["ssh_password"] = ssh_conn.password

    with NamedTemporaryFile() as keyfile:
        extra = ssh_conn.extra
        extra_dejson = ssh_conn.extra_dejson
        if extra:
            if extra_dejson:
                keyfile.write(extra_dejson.get("extra__private_key", "").encode())
            else:
                keyfile.write(extra.encode())
            keyfile.flush()
            kwargs["ssh_pkey"] = os.path.abspath(keyfile.name)
        ssh_tunnel_forwarder = sshtunnel.SSHTunnelForwarder(**kwargs)
        ssh_tunnel_forwarder.start()

    remote_conn.host = "localhost"
    remote_conn.port = ssh_tunnel_forwarder.local_bind_port

    if custom_timeout:
        sshtunnel.TUNNEL_TIMEOUT = previous_timeout

    return ssh_tunnel_forwarder, remote_conn
