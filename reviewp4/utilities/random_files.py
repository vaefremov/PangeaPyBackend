# Utility methods to generate body of files with random content
import os
import pathlib
import logging
import shutil

LOG = logging.getLogger(__name__)

from ..settings import TEMP

MAX_NAMES = 1000

def create_single_file(parent_dir: pathlib.Path, nm: str, sz: int) -> None:
    path = parent_dir / nm
    prefix = nm.encode('utf8') + b' ' + str(sz).encode('utf8') + b'\n'
    with open(path, mode='wb') as f:
        f.write(prefix)
        f.write(os.urandom(sz))

def create_random_files(sz:int, n: int):
    work_dir = pathlib.Path(TEMP).joinpath('RandomFiles')
    work_dir.mkdir(exist_ok=True)
    cur_dir = None
    for i in range(n):
        if (i % MAX_NAMES) == 0:
            cur_dir = work_dir / str(i)
            cur_dir.mkdir(exist_ok=True)
        LOG.debug('Creating %s in %s', i, cur_dir)
        create_single_file(cur_dir, 'random_{}'.format(i), sz)

def create_files_constant_distr(sz: int, n: int, width: float):
    pass

def clear():
    """Remove the work dir with all its content"""
    work_dir = pathlib.Path(TEMP) / 'RandomFiles'
    shutil.rmtree(work_dir)

def files_names_iter():
    """Iterate over all (regular) files in work dir"""
    work_dir = pathlib.Path(TEMP) / 'RandomFiles'
    for f in work_dir.rglob('*'):
        if f.is_dir():
            continue
        yield str(f.absolute())

def cache_status():
    res = {}
    work_dir = pathlib.Path(TEMP) / 'RandomFiles'
    res['exists'] = work_dir.exists()
    if work_dir.exists():
        max_sz = 0
        min_sz = 4_000_000_000
        tot_sz = 0
        n = 0
        for f in work_dir.rglob('*'):
            if f.is_dir():
                continue
            sz = f.stat().st_size
            max_sz = max(sz, max_sz)
            min_sz = min(sz, min_sz)
            tot_sz += sz
            n += 1
        res['max_sz'] = max_sz
        res['min_sz'] = min_sz
        res['tot_sz'] = tot_sz
        res['n'] = n
    return res