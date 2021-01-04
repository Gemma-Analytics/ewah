from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers_manager import ProvidersManager
from airflow.utils.module_loading import import_string


class EWAHConnection(Connection):
    """Extension of airflow's native Connection."""

    @classmethod
    def get_connection_from_secrets(cls, conn_id):
        """Save the calling hook class as provided when called."""
        conn = super().get_connection_from_secrets(conn_id)
        # conn is returned as airflow.hooks.base.BaseHook, return as cls instead
        conn = cls(
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

        # add hook_cls attribute
        hook_class_name, _, _, _ = ProvidersManager().hooks.get(
            conn.conn_type, (None, None, None, None)
        )
        if hook_class_name:
            conn.hook_cls = import_string(hook_class_name)
        else:
            conn.hook_cls = EWAHBaseHook
        return conn

    def get_hook(self):
        return self.hook_cls(conn=self)

    def __getattr__(self, name):
        """Enable: use custom widgets like attributes of a connection.

        E.g. have a field api_key -> get it like conn.api_key!
        """
        if not name == "hook_cls" and hasattr(self, "hook_cls"):
            if (
                hasattr(self.hook_cls, "get_connection_form_widgets")
                or hasattr(self.hook_cls, "get_ui_field_behaviour")
                or self.hook_cls._ATTR_RELABEL
            ):
                if name in self.hook_cls._ATTR_RELABEL.keys():
                    return getattr(self, self.hook_cls._ATTR_RELABEL[name])
                widgets = self.hook_cls.get_connection_form_widgets()
                if name in widgets.keys():
                    return self.extra_dejson.get(name)
                long_name = "extra__" + self.conn_type + "__" + name
                if long_name in widgets.keys():
                    return self.extra_dejson.get(long_name)
        if hasattr(super(), "__getattr__"):
            return super().__getattr__(name)
        _msg = "{0} is not an attribute of {1}!"
        _msg += " conn_id: {2}, conn_type: {3}, hook class: {4}"
        raise AttributeError(
            _msg.format(
                name,
                self.__class__.__name__,
                # If accessed directly, attributes would cause infinite loop
                self.__dict__.get("conn_id"),
                self.__dict__.get("conn_type"),
                self.__dict__.get("hook_cls"),
            )
        )


class EWAHBaseHook(BaseHook):
    """Extension of airflow's native Base Hook."""

    # Overwrite in child class to relabel attributes
    # e.g. {"api_key": "password"} to get self.password for self.api_key
    _ATTR_RELABEL = {}

    def __init__(self, conn: EWAHConnection, *args, **kwargs):
        self.conn = conn
        return super().__init__(*args, **kwargs)

    @classmethod
    def get_connection(cls, conn_id: str) -> EWAHConnection:
        """
        Overwrite classmethod to use extended Connection object.

        Get connection, given connection id.
        :param conn_id: connection id
        :return: connection
        """
        return EWAHConnection.get_connection_from_secrets(conn_id)
