from __future__ import print_function, division, absolute_import

import sys

try:
    from importlib import import_module
except ImportError:
    def import_module(name):
        __import__(name)
        return sys.modules[name]


def resolve_qualname(qualname):
    """Retrieve an object given by a fully-qualified path

    >>> resolve_qualname('os.path.curdir')
    '.'

    """
    names = qualname.split('.')
    for index in range(1, len(names)):
        module_name = '.'.join(names[:-index])
        attrs = names[-index:]
        try:
            obj = import_module(module_name)
        except ImportError:
            continue
        for attr in attrs:
            obj = getattr(obj, attr)
        return obj
    raise ValueError('Unable to resolve qualified path: %r' % qualname)
