from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers_manager import ProvidersManager


class EWAHConnection(Connection):
    """Extension of airflow's native Connection."""

    # def get_hook_class(self):
    #    """Return hook class based on conn_type."""
    #    hook_class_name, conn_id_param, _, _ = ProvidersManager().hooks.get(
    #        self.conn_type, (None, None, None, None)
    #    )
    #    self.conn_id_param = conn_id_param
    #    if not hook_class_name:
    #        raise AirflowException(f'Unknown hook type "{self.conn_type}"')
    #    try:
    #        hook_class = import_string(hook_class_name)
    #    except ImportError:
    #        warnings.warn("Could not import %s", hook_class_name)
    #        raise
    #    return hook_class

    # def get_hook(self):
    #    return self.get_hook_class()(**{self.conn_id_param: self.conn_id})

    # @classmethod
    # def get_connection_from_secrets(cls, conn_id, hook_cls=None):
    #    """Save the calling hook class as provided when called."""
    #    conn = super().get_connection_from_secrets(conn_id)
    #    conn.hook_cls = hook_cls or conn.get_hook_class
    #    return conn

    def __getattr__(self, name):
        """Enable: use custom widgets like attributes of a connection."""
        if hasattr(self, "hook_cls"):
            if hasattr(self.hook_cls, "get_connection_form_widgets"):
                widgets = self.hook_cls.get_connection_form_widgets()
                if name in widgets.keys():
                    return self.extra_dejson.get(name)
        super().__getattr__(name)


class EWAHBaseHook(BaseHook):
    """Extension of airflow's native Base Hook."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # @classmethod
    # def get_hook_from_connection_id(cls, conn_id, conn_type=None):
    #    # Initialize the correct hook suiting the connection type
    #    if not conn_type:
    #        raise Exception('Not implemented!')
    #    hook_class_name, conn_id_param, _, _ = ProvidersManager().hooks.get(
    #        conn_type, (None, None, None, None)
    #    )
    #    if not hook_class_name:
    #        raise AirflowException(f'Unknown hook type "{self.conn_type}"')
    #    try:
    #        hook_class = import_string(hook_class_name)
    #    except ImportError:
    #        warnings.warn("Could not import %s", hook_class_name)
    #        raise
    #    return hook_class()(**{conn_id_param: conn_id})

    @classmethod
    def get_connection(cls, conn_id: str) -> EWAHConnection:
        """
        Overwrite classmethod to use extended Connection object.

        Get connection, given connection id.
        :param conn_id: connection id
        :return: connection
        """
        conn = EWAHConnection.get_connection_from_secrets(conn_id)
        if conn.host:
            log.info(
                "Using connection to: id: %s. Host: %s, Port: %s, Schema: %s, Login: %s, Password: %s, "
                "extra: %s",
                conn.conn_id,
                conn.host,
                conn.port,
                conn.schema,
                conn.login,
                "XXXXXXXX" if conn.password else None,
                "XXXXXXXX" if conn.extra else None,  # No need to de-json here!
            )
        return conn
