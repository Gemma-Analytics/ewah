import os, sys
from ewah.hooks.base import EWAHBaseHook

# import all hooks by walking through all files in this directory and
# importing all objects that are subclasses of EWAHBaseHook
# adapted from:
#   https://stackoverflow.com/questions/6246458/import-all-classes-in-directory
relevant_files = [
    f[:-3]
    for f in os.listdir(os.path.dirname(os.path.abspath(__file__)))
    if f.endswith(".py") and not f in ["__init__.py"]
]

hook_class_names = []
name_template = "ewah.hooks.{0}.{1}"

for py_file in relevant_files:
    mod = __import__(".".join([__name__, py_file]), fromlist=[py_file])
    classes = [getattr(mod, x) for x in dir(mod) if isinstance(getattr(mod, x), type)]
    for cls in classes:
        if issubclass(cls, EWAHBaseHook) and not cls == EWAHBaseHook:
            hook_class_names.append(name_template.format(py_file, cls.__name__))

## Sample hook - delete this when all features have been tested

# from typing import Any, Dict, Optional
# from airflow.hooks.dbapi import DbApiHook
# from airflow.models.connection import Connection
#
#
# class JdbcHook(DbApiHook):
#     """
#     General hook for jdbc db access.
#     JDBC URL, username and password will be taken from the predefined connection.
#     Note that the whole JDBC URL must be specified in the "host" field in the DB.
#     Raises an airflow error if the given connection id doesn't exist.
#     """
#
#     conn_name_attr = 'jdbc_conn_id'
#     default_conn_name = 'jdbc_default'
#     conn_type = 'jdbc'
#     hook_name = 'JDBC Connection'
#     supports_autocommit = True
#
#     @staticmethod
#     def get_connection_form_widgets() -> Dict[str, Any]:
#         """Returns connection widgets to add to connection form"""
#         from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
#         from flask_babel import lazy_gettext
#         from wtforms import StringField
#
#         return {
#             "extra__jdbc__drv_path": StringField(lazy_gettext('Driver Path'), widget=BS3TextFieldWidget()),
#             "extra__jdbc__drv_clsname": StringField(
#                 lazy_gettext('Driver Class'), widget=BS3TextFieldWidget()
#             ),
#         }
#
#     @staticmethod
#     def get_ui_field_behaviour() -> Dict:
#         """Returns custom field behaviour"""
#         return {
#             "hidden_fields": ['port', 'schema', 'extra'],
#             "relabeling": {'host': 'Connection URL'},
#         }
#
#     def get_conn(self) -> Dict:
#         conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
#         host: str = conn.host
#         login: str = conn.login
#         psw: str = conn.password
#         jdbc_driver_loc: Optional[str] = conn.extra_dejson.get('extra__jdbc__drv_path')
#         jdbc_driver_name: Optional[str] = conn.extra_dejson.get('extra__jdbc__drv_clsname')
#
#         conn = {
#             "jclassname": jdbc_driver_name,
#             "url": str(host),
#             "driver_args": [str(login), str(psw)],
#             "jars": jdbc_driver_loc.split(",") if jdbc_driver_loc else None,
#         }
#         return conn
