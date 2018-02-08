import traceback

import yaml


class ObjectLoader(object):
    def __init__(self, bindings):
        self.bindings = {}
        self.instances = {}
        for name, conf in bindings.iteritems():
            type = obj_from_path(conf.pop('type'))
            self.bindings[name] = {
                'type': type,
                'kwargs': conf
            }

    def get(self, name):
        if name in self.instances: return self.instances[name]

        kwargs = self.bindings[name]['kwargs']

        for k, v in kwargs.iteritems():
            if isinstance(v, basestring) and v.startswith('$'):
                kwargs[k] = self.get(v[1:])

        res = self.instances[name] = self.bindings[name]['type'](**kwargs)
        return res

    @classmethod
    def from_yaml(cls, fname):
        with open(fname) as f:
            return cls(yaml.load(f))

def obj_from_path(path):
    """
    Retrieves an object from a given import path. The format is slightly different from the standard python one in
    order to be more expressive.
    Examples:
    >>> obj_from_path('pandas')
    <module 'pandas'>
    >>> obj_from_path('fito.specs')
    <module 'pandas.core'>
    >>> obj_from_path('pandas.core.series:Series')
    pandas.core.series.Series
    >>> obj_from_path('pandas.core.series:Series.abs')
    <unbound method Series.abs>
    """
    parts = path.split(':')
    assert len(parts) <= 2

    obj_path = []
    full_path = parts[0]
    if len(parts) == 2:
        obj_path = parts[1].split('.')

    fromlist = '.'.join(full_path.split('.')[:-1])

    try:
        module = __import__(full_path, fromlist=fromlist)
    except ImportError:
        traceback.print_exc()
        raise RuntimeError("Couldn't import {}".format(path))

    obj = module
    for i, attr in enumerate(obj_path):
        obj = getattr(obj, attr)
    return obj
