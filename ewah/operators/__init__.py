import os, sys
from ewah.operators.base_operator import EWAHBaseOperator

# import all operators
# adapted from:
#   https://stackoverflow.com/questions/6246458/import-all-classes-in-directory
relevant_files = [
    f[:-3] for f in os.listdir(os.path.dirname(os.path.abspath(__file__)))
    if f.endswith('.py') and not f in ['base_operator.py', '__init__.py']
]

for py_file in relevant_files:
    mod = __import__('.'.join([__name__, py_file]), fromlist=[py_file])
    classes = [
        getattr(mod, x) for x in dir(mod)
        if isinstance(getattr(mod, x), type)
    ]
    for cls in classes:
        if issubclass(cls, EWAHBaseOperator) and not cls == EWAHBaseOperator:
            setattr(sys.modules[__name__], cls.__name__, cls)
