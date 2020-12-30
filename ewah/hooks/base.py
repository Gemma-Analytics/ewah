from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.utils.module_loading import import_string
from airflow.providers_manager import ProvidersManager


class EWAHConnection(Connection):
    """Extension of airflow's native Connection."""

    @classmethod
    def get_connection_from_secrets(cls, conn_id):
        """Save the calling hook class as provided when called."""
        conn = super().get_connection_from_secrets(conn_id)
        # conn is returned as airflow.hooks.base.BaseHook, return as cls instead
        return cls(
            conn_id=conn.conn_id,
            conn_type=conn.conn_type,
            description=conn.description,
            host=conn.host,
            login=conn.login,
            password=conn.password,
            schema=conn.schema,
            port=conn.port,
            extra=conn.extra,
        )

    def __getattr__(self, name):
        """Enable: use custom widgets like attributes of a connection.

        E.g. have a field api_key -> get it like conn.api_key!
        """
        if not name == "hook_cls": # Otherwise causes an infinite loop
            if hasattr(self.hook_cls, "get_connection_form_widgets"):
                if name in self.hook_cls._ATTR_RELABEL.keys():
                    return getattr(self, self.hook_cls._ATTR_RELABEL[name])
                widgets = self.hook_cls.get_connection_form_widgets()
                if name in widgets.keys():
                    return self.extra_dejson.get(name)
                long_name = "extra__" + self.conn_type + "__" + name
                if long_name in widgets.keys():
                    return self.extra_dejson.get(long_name)
        if hasattr(super(), '__getattr__'):
            return super().__getattr__(name)
        raise AttributeError("{0} is not an attribute of {1}!".format(
            name, self.__class__.__name__,
        ))


class EWAHBaseHook(BaseHook):
    """Extension of airflow's native Base Hook."""

    # Overwrite in child class to relabel attributes
    # e.g. {"api_key": "password"} to get self.password for self.api_key
    _ATTR_RELABEL = {}

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
        hook_class_name, _, _, _ = ProvidersManager().hooks.get(
            conn.conn_type, (None, None, None, None)
        )
        # if just using cls, it would use the wrong class as hook_cld if
        # method is called directly from EWAHBaseHook!
        if hook_class_name:
            conn.hook_cls = import_string(hook_class_name)
        return conn
