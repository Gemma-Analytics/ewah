from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.providers_manager import ProvidersManager
from airflow.utils.module_loading import import_string

from typing import Type, Optional


class EWAHConnection(Connection):
    """Extension of airflow's native Connection."""

    @classmethod
    def get_cleaner_callables(cls):
        # overwrite me for cleaner callables that are always called
        return []

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
        hook_class_name = ProvidersManager().hooks.get(
            conn.conn_type, (None, None, None, None)
        )[0]
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

    def __init__(
        self,
        conn: Optional[EWAHConnection] = None,
        conn_id: Optional[str] = None,
        *args,
        **kwargs
    ):
        if not (conn or conn_id):
            raise Exception("Must supply either 'conn' or 'conn_id'!")
        if conn and conn_id:
            raise Exception("Must not supply both 'conn' or 'conn_id'!")
        if conn_id:
            conn = self.get_connection(conn_id)
        self.conn = conn

        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        elif len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])

        return super().__init__()

    @classmethod
    def get_connection(cls, conn_id: str) -> EWAHConnection:
        """
        Overwrite classmethod to use extended Connection object.

        Get connection, given connection id.
        :param conn_id: connection id
        :return: connection
        """
        return EWAHConnection.get_connection_from_secrets(conn_id)

    @classmethod
    def get_hook_from_conn_id(cls, conn_id: str):
        conn = cls.get_connection(conn_id=conn_id)
        return conn.hook_cls(conn=conn)
