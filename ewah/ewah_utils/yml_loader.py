import os
import yaml

from jinja2 import Template
from io import StringIO

try:
    from yaml import CLoader as base_loader, CDumper as Dumper
except ImportError:
    from yaml import Loader as base_loader, Dumper

"""
Extend the loader class to be able to reference files in .yaml files.
Adapted from https://stackoverflow.com/questions/528281/how-can-i-include-a-yaml-file-inside-another
Accessed 2020-07-28
"""


class Loader(base_loader):
    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        # Enable Jinja2 in the yaml files
        yaml_stream = StringIO(Template(stream.read()).render())
        yaml_stream.name = stream.name
        super().__init__(yaml_stream)

    def scalar_from_file(self, node):
        filename = os.path.join(self._root, self.construct_scalar(node))
        with open(filename, "r") as f:
            return f.read()

    def include_other_yml(self, node):
        filename = os.path.join(self._root, self.construct_scalar(node))
        with open(filename, "r") as f:
            return yaml.load(f, Loader)


Loader.add_constructor("!text_from_file", Loader.scalar_from_file)
Loader.add_constructor("!yml_from_file", Loader.include_other_yml)
