import os, sys
from ewah.operators.base import EWAHBaseOperator

# import all operators by walking through all files in this directory and
# importing all objects that are subclasses of EWAHBaseOperator
# adapted from:
#   https://stackoverflow.com/questions/6246458/import-all-classes-in-directory
relevant_files = [
    f[:-3]
    for f in os.listdir(os.path.dirname(os.path.abspath(__file__)))
    if f.endswith(".py") and not f in ["__init__.py"]
]

operator_list = {}

for py_file in relevant_files:
    mod = __import__(".".join([__name__, py_file]), fromlist=[py_file])
    classes = [getattr(mod, x) for x in dir(mod) if isinstance(getattr(mod, x), type)]
    for cls in classes:
        if issubclass(cls, EWAHBaseOperator) and not cls == EWAHBaseOperator:
            names = cls._NAMES
            if isinstance(names, str):
                names = [names]
            for name in names:
                operator_list.update({name: cls})
