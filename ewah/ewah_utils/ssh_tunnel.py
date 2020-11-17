"""
    A utility to use SSH tunnel port forwarding including SSH agent forwarding.
    The motivation is that existing packages either do SSH tunnel forwarding
    well (`sshtnnel.SSHTunnelForwarder`) or they do SSH agent forwarding
    (`paramiko`), but no out of the box solution can provide both. EWAH
    uses this utility to implement SSH tunneling with and without SSH agent
    forwarding across all operators.

    Inspiration for the utility came from:
    - This stack overflow post: https://stackoverflow.com/questions/23666600/ssh-key-forwarding-using-python-paramiko
    - This implementation of SSH port forwarding: https://github.com/paramiko/paramiko/blob/master/demos/forward.py
    - This implementation of SSH agent forwarding: https://gist.github.com/toejough/436540622530c35404e6
"""

from airflow.hooks.base_hook import BaseHook

import getpass
import os
import socket
import select
import socketserver
import sys
import paramiko
import threading

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
        sshtunnel.TUNNEL_TIMEOUT = custom_timeout
    ssh_conn = BaseHook.get_connection(ssh_conn_id)
    remote_conn = BaseHook.get_connection(remote_conn_id)
    kwargs = {
        'ssh_address_or_host': (ssh_conn.host, ssh_conn.port or 22),
        'remote_bind_address': (remote_conn.host, remote_conn.port),
    }
    if ssh_conn.login:
        kwargs['ssh_username'] = ssh_conn.login or None
    if ssh_conn.password:
        kwargs['ssh_password'] = ssh_conn.password

    with NamedTemporaryFile() as keyfile:
        extra = ssh_conn.extra
        if extra:
            keyfile.write(extra.encode())
            keyfile.flush()
            kwargs['ssh_pkey'] = os.path.abspath(keyfile.name)
        ssh_tunnel_forwarder = sshtunnel.SSHTunnelForwarder(**kwargs)
        ssh_tunnel_forwarder.start()

    remote_conn.host = 'localhost'
    remote_conn.port = ssh_tunnel_forwarder.local_bind_port

    return ssh_tunnel_forwarder, remote_conn
