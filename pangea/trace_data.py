# -*- coding: utf-8 -*-
# $Id: $
""" Objects related to representing seismic 2D and 3D data.
"""

from collections import namedtuple
import numpy as np
from abc import ABCMeta, abstractmethod

import pangea.dxcube
import pangea.dxline
import pangea.np_utils
import math
import logging

logger = logging.getLogger(__name__)

class Trace(namedtuple('Trace', ['i', 'j', 'x', 'y', 'z0', 'dz', 'data'])):
    """
    Class to represent single trace of data taken from seismic profile or cube.
    Here i, j - indices of trace (inline/cross-line or trace number);
    x, y - coordinates of the trace;
    z0 - starting value of the 3-d axis, dz - the corresponding increment;
    data - np.array of float64
    data[0] corresponds to z0, data[1] ~ z0+dz, ...
    """
    __slots__ = ()


class Axis(namedtuple('Axis', ['origin', 'step', 'n_points'])):
    """
    Class representing axis in one dimension (usually depth or travel time).
    origin and step should be doubles, n_points - integer.
    """
    __slots__ = ()

    def sample_points(self):
        return np.linspace(self.origin, self.origin + self.step * self.n_points, num=self.n_points,
                           endpoint=False, dtype=np.float64)

    def last_point_coordinate(self):
        assert self.n_points > 0
        return self.origin + (self.n_points - 1) * self.step

    def z_to_index(self, z_coord):
        """Converts coordinate of point on axis to the corresponding index.
        """
        i = int(round((z_coord - self.origin) / self.step))
        if (i < 0) or (i >= self.n_points):
            raise IndexError('Z coordinate is outside of the range of axis values (%g, %g, %d)' % (self.origin, z_coord, self.last_point_coordinate()))
        return i


class TraceDataReader(metaclass=ABCMeta):
    """
    Abstract class (interface) representing reader of trace data.
    """
    def __init__(self, object_name='Unnamed Absctract Trace reader'):
        self._name=object_name

    @abstractmethod
    def trace_at(self, i, j):
        pass

    @abstractmethod
    def trace_at_xy(self, x, y):
        pass

    @abstractmethod
    def next_trace(self):
        yield None

    @property
    @abstractmethod
    def trace_count(self):
        return 0

    @property
    @abstractmethod
    def count(self):
        return 0

    @property
    @abstractmethod
    def z_axis(self):
        return None

    @property
    @abstractmethod
    def geometry(self):
        return None

    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def reopen(self):
        pass

    @abstractmethod
    def is_geometry_same(self, other):
        pass

    @property
    def name(self):
        return self._name

    def __repr__(self):
        return '"{}"'.format(self._name)

class TraceDataWriter(metaclass=ABCMeta):
    """
    Interface representing class able to write traces to the corresponding trace data set.
    """

    @abstractmethod
    def put_trace(self, t):
        pass


class SeisCubeReader(TraceDataReader):
    def __init__(self, file_in=None, object_name=None):
        self.dx_cube = pangea.dxcube.DXCube()
        self.time_axis = None
        self._name = object_name or 'Unnamed SeisCubeReader'
        if file_in:
            self.dx_cube.attach_to_file(file_in)
            t0, dt, n = self.dx_cube.time_axis()
            self.time_axis = Axis(origin=t0, step=dt, n_points=n)

    def close(self):
        self.dx_cube.close()

    def reopen(self):
        self.dx_cube.reopen()
        return self

    @property
    def geometry(self):
        return self.dx_cube.geometry()

    def trace_at_xy(self, x, y):
        i, j = self.dx_cube.xy_to_inline_xline(x, y)
        return self.trace_at(i, j)

    @property
    def trace_count(self):
        return self.dx_cube.number_of_traces()

    @property
    def count(self):
        return self.dx_cube.number_of_traces() * self.time_axis.n_points

    def trace_at(self, i, j):
        data = self.dx_cube.get_trace_by_numbers_asarray(i, j)
        x, y = self.dx_cube.inl_xln_coordinates((i, j))
        return Trace(i=i, j=j, x=x, y=y, z0=self.time_axis.origin, dz=self.time_axis.step, data=data)

    def next_trace(self):
        for i, j in self.dx_cube.traces_numbers_iter():
            yield self.trace_at(i, j)

    @property
    def z_axis(self):
        return self.time_axis

    def is_geometry_same(self, other):
        return is_cube_geometry_same(self.geometry, other.geometry)


class SeisLineReader(TraceDataReader):
    def __init__(self, file_name=None, object_name=None):
        self.dx_line = pangea.dxline.DxLine()
        if file_name:
            self.dx_line.attach_to_file(file_name)
        t0, dt, n = self.dx_line.time_axis()
        self.time_axis = Axis(origin=t0, step=dt, n_points=n)
        self._name = object_name or 'Unnamed SeisLineReader'

    def close(self):
        self.dx_line.close()

    def reopen(self):
        self.dx_line.reopen()
        return self

    @property
    def geometry(self):
        return self.dx_line.geometry()

    def trace_at_xy(self, x, y):
        # @TODO: implement getting index basing on coordinates of trace
        i = None
        return self.trace_at(i)

    @property
    def trace_count(self):
        return self.dx_line.n_traces

    @property
    def count(self):
        return self.dx_line.n_traces * self.time_axis.n_points

    def trace_at(self, i, j=0):
        data = self.dx_line.np_get_ith_trace(i)
        # @TODO: implement getting trace coordinates
        # x, y = self.dx_cube.inl_xln_coordinates(i, j)
        x, y, _ = self.dx_line.geometry()[i]
        return Trace(i=i, j=0, x=x, y=y, z0=self.time_axis.origin, dz=self.time_axis.step, data=data)

    def next_trace(self):
        for i in range(self.dx_line.n_traces):
            yield self.trace_at(i)

    @property
    def z_axis(self):
        return self.time_axis

    def is_geometry_same(self, other):
        return is_line_geometry_same(self.geometry, other.geometry)


class SeisCubeWriter(SeisCubeReader, TraceDataWriter):
    def __init__(self, file_name, geometry_from=None, geom=None, time_axis=None, object_name=None):
        self.time_axis = time_axis
        self.dx_cube = pangea.dxcube.DXCubeWriter(geom=geom, time_axis=time_axis,
                                                  filename=file_name, object_name=object_name)
        self._name = object_name or 'Unnamed SeisCubeWriter'

    def put_trace(self, t):
        self.dx_cube.np_write_trace_at_ij(t.data, t.i, t.j)


class SeisLineWriter(SeisLineReader, TraceDataWriter):
    def __init__(self, file_name, geom=None, time_axis=None, object_name=None):
        self.time_axis = time_axis
        self.dx_line = pangea.dxline.DXLineWriter(geom=geom, time_axis=time_axis,
                                                  filename=file_name, object_name=object_name)
        self._name = object_name or 'Unnamed SeisLineWriter'

    def put_trace(self, t):
        self.dx_line.np_write_ith_trace(t.data, t.i)


def make_empty_trace_like(t):
    """
    Makes an empty trace like the given one.

    :type t: Trace
    :param t:
    :return:
    """
    data = np.full_like(t.data, pangea.np_utils.MAXFLOAT, dtype=np.float64)
    return Trace(t.i, t.j, t.x, t.y, t.z0, t.dz, data)


def is_cube_geometry_same(g, g1, accuracy=1.0e-2):
    """
    Returns True if two cube or line geometries coincide
    :param g:
    :param g1:
    :param accuracy:
    :return: True or False
    """

    def manhattan_norm_2d(v1, v2):
        return max(abs(v1[0] - v2[0]), abs(v1[1] - v2[1]))

    def mult(v, n):
        return v[0]*n, v[1]*n

    origin, v_i, v_x, n_i, n_x = g
    origin1, v_i1, v_x1, n_i1, n_x1 = g1
    if n_i != n_i1:
        logger.error("n_inlines incompatible: %d %d", n_i, n_i1)
        return False
    if n_x != n_x1:
        logger.error("n_xnlines incompatible: %d %d", n_x, n_x1)
        return False
    if manhattan_norm_2d(origin, origin1) > accuracy:
        logger.error('Incompatible origins %g', manhattan_norm_2d(origin, origin1))
        return False
    if manhattan_norm_2d(mult(v_x, n_x), mult(v_x1, n_x1)) > accuracy:
        logger.error('Incompatible v_xline %g', manhattan_norm_2d(v_x, v_x1))
        return False
    if manhattan_norm_2d(mult(v_i, n_i), mult(v_i1, n_i1)) > accuracy:
        logger.error('Incompatible v_inline %g', manhattan_norm_2d(v_i, v_i1))
        return False
    return True


def is_line_geometry_same(g1, g2, accuracy=1.0e-2):
    """
    Returns True if two line geometries are the same
    :param g1:
    :param g2:
    :param accuracy:
    :return:
    """

    def manhattan_norm_2d(v1, v2):
        return max(abs(v1[0] - v2[0]), abs(v1[1] - v2[1]))

    if len(g1) != len(g2):
        logger.error('Incompatible lengths of lines: %d %d', len(g1), len(g2))
        return False
    max_diff = max([manhattan_norm_2d(p[0], p[1]) for p in zip(g1, g2)])
    if max_diff > accuracy:
        logger.error('Incompatible geometries, max. difference %g', max_diff)
        return False
    return True


def is_axis_same(a1: Axis, a2: Axis, accuracy=1.0e-6):
    """
    Returns true if two axis are the same
    :param a1:
    :param a2:
    :return:
    """
    try:
        if abs(np.max(a1.sample_points() - a2.sample_points())) > accuracy:
            return False
    except ValueError:
        return False
    return True


def intersect_axes(*axes_list):
    def f_round(x):
        return math.floor(x + 0.5)

    new_step = axes_list[0].step
    # all the steps should be the same
    assert all(abs(new_step - i.step) < 0.001 * new_step for i in axes_list), 'Axes steps differ'
    # all origins differences should be multiples of step
    assert max([abs(f_round((i.origin - j.origin) / new_step) -
                    (i.origin - j.origin) / new_step) for i in axes_list for j in axes_list]) < 0.001, \
        'Origin differences are not multiples of step'
    # calculate new_origin as max(origins)
    new_origin = max(a.origin for a in axes_list)
    # calculate last point as min of last points
    new_last_point = min(a.last_point_coordinate() for a in axes_list)
    new_n_points = round((new_last_point - new_origin) / new_step) + 1
    assert new_n_points > 0, 'Axes do not intersect!'
    res_axis = Axis(origin=new_origin, step=new_step, n_points=new_n_points)
    # calculate start & end indices
    ij = [(round((new_origin - a.origin ) / new_step),
           a.n_points - round((a.last_point_coordinate() - res_axis.last_point_coordinate()) / new_step)) for a in axes_list]
    return res_axis, ij
