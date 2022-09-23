# Utility methods to generate body of files with random content
import os
import pathlib
import logging
import shutil
import random
import math
from typing import Generator

LOG = logging.getLogger(__name__)

from ..settings import TEMP

MAX_NAMES = 1000

def create_single_file(parent_dir: pathlib.Path, nm: str, sz: int) -> None:
    path = parent_dir / nm
    prefix = nm.encode('utf8') + b' ' + str(sz).encode('utf8') + b'\n'
    with open(path, mode='wb') as f:
        f.write(prefix)
        f.write(os.urandom(sz))

def const_size_iter(sz: int, width: float) -> Generator[int, None, None]:
    while True:
        yield sz

def equal_distr_iter(sz: int, width: float) -> Generator[int, None, None]:
    start, stop = max(0, int(sz - sz*width)), int(sz + sz*width)+1
    while True:
        yield random.randrange(start, stop)

def gauss_distr_iter(sz: int, width: float) -> Generator[int, None, None]:
    while True:
        v = math.floor(random.gauss(sz, sz*width) + 0.5)
        if v < 0:
            continue
        yield v

def create_random_files_with_distr(sz:int, n: int, distr_name: str, width: float):
    if distr_name == 'const':
        gen = const_size_iter(sz, width)
    elif distr_name == 'equal':
        gen = equal_distr_iter(sz, width)
    elif distr_name == 'gauss':
        gen = gauss_distr_iter(sz, width)
    else:
        raise RuntimeError('Unknown distribution')
    work_dir = pathlib.Path(TEMP).joinpath('RandomFiles')
    work_dir.mkdir(exist_ok=True)
    cur_dir = None
    for i in range(n):
        if (i % MAX_NAMES) == 0:
            cur_dir = work_dir / str(i)
            cur_dir.mkdir(exist_ok=True)
        cur_sz = next(gen)
        LOG.debug('Creating %s in %s sz=%s', i, cur_dir, cur_sz)
        create_single_file(cur_dir, 'random_{}'.format(i), cur_sz)

def clear():
    """Remove the work dir with all its content"""
    work_dir = pathlib.Path(TEMP) / 'RandomFiles'
    shutil.rmtree(work_dir, ignore_errors=True)

def files_names_iter():
    """Iterate over all (regular) files in work dir"""
    work_dir = pathlib.Path(TEMP) / 'RandomFiles'
    for f in work_dir.rglob('*'):
        if f.is_dir():
            continue
        yield str(f.absolute())

def cache_status(n_channels: int=10):
    res = {}
    work_dir = pathlib.Path(TEMP) / 'RandomFiles'
    res['exists'] = work_dir.exists()
    if work_dir.exists():
        max_sz = 0
        min_sz = 4_000_000_000
        tot_sz = 0
        n = 0
        sizes = []
        for f in work_dir.rglob('*'):
            if f.is_dir():
                continue
            sz = f.stat().st_size
            max_sz = max(sz, max_sz)
            min_sz = min(sz, min_sz)
            tot_sz += sz
            n += 1
            sizes.append(sz)
        res['max_sz'] = max_sz
        res['min_sz'] = min_sz
        res['tot_sz'] = tot_sz
        res['n'] = n
        res['histogram'] = histogram(min_sz, max_sz, n_channels, sizes)
    return res

def histogram(start, end, n_channels, seq):
    step = (end - start) / n_channels
    if step == 0.0:
        step = 1.0
    res = {'n_channels': n_channels, 'values': [0 for _ in range(n_channels)], 'step': step}
    for v in seq:
        i = math.floor((v - start) / step)
        if i < 0:
            i = 0
        if i >= n_channels:
            i = max(0, n_channels-1)
        res['values'][i] += 1
    return res
