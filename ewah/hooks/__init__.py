import os, sys
from ewah.hooks.base import EWAHBaseHook
from ewah.hooks.sql_base import EWAHSQLBaseHook

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
        if (
            issubclass(cls, EWAHBaseHook)
            and not cls == EWAHBaseHook
            and not cls == EWAHSQLBaseHook
        ):
            hook_class_names.append(name_template.format(py_file, cls.__name__))
