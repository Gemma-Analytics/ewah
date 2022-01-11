from collections.abc import Iterable
import six


def is_iterable_not_string(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, six.string_types)
