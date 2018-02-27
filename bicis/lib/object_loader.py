import os
import traceback
import warnings

import luigi
import yaml


class ObjectLoader(luigi.Config):
    fname = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super(ObjectLoader, self).__init__(*args, **kwargs)

        with open(self.fname) as f:
            bindings = yaml.load(f)

        self.bindings = {}
        self.instances = {}
        for name, conf in bindings.iteritems():
            if isinstance(conf, dict):
                self.bindings[name] = {
                    'type': conf.pop('type'),
                    'kwargs': conf
                }
            else:
                self.bindings[name] = conf

    def get(self, name):
        if name in self.instances: return self.instances[name]

        binding = self.bindings[name]
        kwargs = {}

        for k, v in binding['kwargs'].iteritems():
            if isinstance(v, basestring) and v.startswith('$'):
                kwargs[k] = self.get(v[1:])

        binding_type = obj_from_path(binding['type'])
        res = self.instances[name] = binding_type(**kwargs)
        return res

    @property
    def experiment_name(self):
        # the experiment_name defaults to the name of the file
        return self.bindings.get('experiment_name', os.path.basename(self.fname.replace('.yaml', '')))

    @classmethod
    def from_yaml(cls, fname):
        warnings.warn('DEPRECATED')
        return cls(fname)



def obj_from_path(path):
    """
    Retrieves an object from a given import path. The format is slightly different from the standard python one in
    order to be more expressive.
    Examples:
    >>> obj_from_path('pandas')
    <module 'pandas'>
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

object_loader = ObjectLoader()
