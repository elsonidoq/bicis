import os

from filelock import FileLock

def check_if_exists(path):
    if not os.path.exists(path): os.makedirs(path)
    return path

here = os.path.dirname(__file__)
data_dir = check_if_exists(os.path.join(here, '../../data'))
locks_dir = check_if_exists(os.path.join(here, '../../locks'))

trajectories_dir = os.path.join(data_dir, 'bicicletas-publicas/')
stations_dir = os.path.join(data_dir, 'estaciones-bicicletas-publicas')


scripts_dir = os.path.join(here, '../../scripts')



def get_lock(obj):
    fname = os.path.join(locks_dir, type(obj).__name__)
    return FileLock(fname)

