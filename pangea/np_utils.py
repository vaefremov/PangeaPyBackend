# -*- coding: utf-8 -*-
# $Id: $
""" Utilities that can be used in data processing routines.
"""

import numpy as np

__author__ = 'efremov'

MAXFLOAT = 3.40282347e+38  # stands for undefined values of parameters
MAXFLOAT09 = 0.9 * 3.40282347e+38  # stands for undefined values of parameters


def generate_test_trace(length, i_start=0, i_end=None, mean=100.):
    """
    Generate test trace data (array) where values outside the [i_start:i_end] range
    are undefs (MAXFLOAT)
    :param length: total length of the output array
    :param i_start: index where the non-undefs start
    :param i_end: index of the last non-undef element, length-1 if None
    :param mean: mean value of data
    :return: the generated 1D array
    """
    res = np.random.normal(mean, size=length)
    res[:i_start] = MAXFLOAT
    if i_end is None:
        i_end = length
    res[i_end:] = MAXFLOAT
    return res


def zero_start_end_undefs(y):
    """
    Replace starting and trailing undefs with zero. Affects the input array.
    :param y: 1D array
    :return: Tuple (i_start, i_end, zeroed_aray).
    """
    i_start = np.argmax(y < MAXFLOAT09)
    i_end = len(y) - np.argmax(y[::-1] < MAXFLOAT09)
    y[:i_start] = 0.0
    y[i_end:] = 0.0
    return i_start, i_end, y

def zero_start_end_undefs_block(d):
    '''
    Accepts array of shape (n_data, n_points), where the second dimension corresponds to input traces. Zeroes
    undefs at the start and end of every trace, returns the original block of traces with zeroed undefs, and
    list of tuples containing starting and ending indices of data points for every trace.
    :param d:
    :return: tuple (zeroed_data, [(i_start, i_end), ....])
    '''
    assert len(d.shape) == 2, 'Block of traces must have the (n_data, n_points)shape!'
    n_data = d.shape[0]
    ind_out = []
    for i in range(n_data):
        i_start, i_end, _ = zero_start_end_undefs(d[i])
        ind_out.append((i_start, i_end))
    return d, ind_out


def has_undefs(y):
    """
    Return true if the array has undefs
    :param y:
    :return:
    """
    return len(np.where(y > MAXFLOAT09)[0]) != 0


def make_empty_trace(length):
    """
    Makes array filled with MAXFLOATS
    :param length: the required length of the array
    :return: output np.array
    """
    return np.full((length,), MAXFLOAT, dtype=np.float64)

def make_empty_block(n_traces, length):
    """
    Makes array filled with MAXFLOATS
    :param length: the required length of the array
    :return: output np.array
    """
    return np.full((n_traces, length), MAXFLOAT, dtype=np.float64)
