# -*- coding: utf-8 -*-
# $Id: $
""" Module implementing representation of planar objects (including 1d-2d horizons).
"""

from collections import namedtuple
import numpy as np
from abc import ABCMeta, abstractmethod
import pangea.dxextractobj
from pangea.dxextractobj import Horizon3DGeometry
import pangea.np_utils
import logging

MAXFLOAT = 3.40282347e+38  # stands for undefined values of parameters
MAXFLOAT09 = 0.9 * 3.40282347e+38  # stands for undefined values of parameters


class PlanarValue(namedtuple('PlanarValue', 'z val')):
    __slots__ = ()


LOGGER = logging.getLogger(__name__)


class PlanarCommonInterface(metaclass=ABCMeta):
    """Interface representing readers of planars and horizons in 2D and 3D case.
    """

    def __init__(self, object_name='Unnamed planar'):
        self._name = object_name

    @abstractmethod
    def at_xy(self, x, y, accuracy=1.):
        """Find value (depth/time or planar value) corresponding to x,y coordinates of point.
        May return undefined value appropriate for a concrete implementation type.
        """
        pass

    @abstractmethod
    def at_ij(self, i, j=0):
        """Find value (depth/time or planar value) corresponding to numbers of point in the
        objects' geometry. May return undefined value appropriate for a concrete
        implementation type or throw IndexError exception.
        """
        pass

    @property
    def name(self):
        """Returns name of the object.
        """
        return self._name

    def __repr__(self):
        return '"{}"'.format(self._name)


class PlanarWriter(metaclass=ABCMeta):
    """Interface that must be implemented by planars/horizons writers.
    """
    def __init__(self):
        self.is_closed = False

    @abstractmethod
    def add_point_ij(self, val: PlanarValue, i, j=0):
        """
        Sets value for the point located by indices i,j (inline-xline).
        In 2D case the value of j should be ignored (and should have a default value)
        :parameter: val may be either float or PlanarValue
        """
        if self.is_closed:
            raise RuntimeError('Object is closed')

    @abstractmethod
    def close(self):
        """
            Writes data to the file. Further calls of add_point_* methods should raise
            exception.
        """
        self.is_closed = True


class Planar2D(PlanarCommonInterface):

    def __init__(self, file_name, object_name='Unnamed Planar 2D'):
        super().__init__(object_name)
        if file_name is None:
            self.xy = np.array([])
            self.z = np.array([])
            self.val = np.array([])
        else:
            dx_planar = pangea.dxextractobj.Planar2DFromDX(file_name)
            raw_data = dx_planar.getRawGeometry()
            self.xy = np.array([(i[0], i[1]) for i in raw_data])
            self.z = np.array([i[3] for i in raw_data])
            self.val = np.array([i[4] for i in raw_data])

    def at_ij(self, i, j=0):
        return PlanarValue(self.z[i], self.val[i])

    def at_xy(self, x, y, accuracy=5.):
        min_ind = np.argmin(np.hypot((self.xy[:, 0] - x), (self.xy[:, 1] - y)))
        min_dist = np.hypot((self.xy[min_ind, 0] - x), (self.xy[min_ind, 1] - y))
        if min_dist > accuracy:
            LOGGER.warning('Minimal distance from the given point to the line exceeds accuracy: %g', min_dist)
            return PlanarValue(MAXFLOAT, MAXFLOAT)
        return self.at_ij(min_ind)


class PlanarWriter2D(Planar2D, PlanarWriter):
    def __init__(self, file_name, geometry, object_name='Unnamed Planar Writer 2D'):
        super().__init__(file_name=None, object_name=object_name)
        PlanarWriter.__init__(self)
        self.xy = np.array([(i[0], i[1]) for i in geometry])
        self.z = pangea.np_utils.make_empty_trace(len(geometry))
        self.val = pangea.np_utils.make_empty_trace(len(geometry))
        self.dx_planar = pangea.dxextractobj.Planar2DFromDXWriter(file_name, geometry, self.z, self.val)
        
    def add_point_ij(self, val: PlanarValue, i, j=None):
        if self.is_closed:
            raise RuntimeError('Object is closed')
        self.z[i] = val.z
        self.val[i] = val.val

    def close(self):
        self.dx_planar.flush()
        self.is_closed = True

class Horizon2D(PlanarCommonInterface):
    def __init__(self, filename, object_name='Unnamed Horizon 2D'):
        super().__init__(object_name)
        dx_horizon = pangea.dxextractobj.Horizon2DFromDX(filename)
        raw_data = dx_horizon.getRawGeometry()
        self.xy = np.array([(i[0], i[1]) for i in raw_data])
        self.z = np.array([i[3] for i in raw_data])

    def at_ij(self, i, j=0):
        return self.z[i]

    def at_xy(self, x, y, accuracy=5.):
        min_ind = np.argmin(np.hypot((self.xy[:, 0] - x), (self.xy[:, 1] - y)))
        min_dist = np.hypot((self.xy[min_ind, 0] - x), (self.xy[min_ind, 1] - y))
        if min_dist > accuracy:
            LOGGER.warning('Minimal distance from the given point to the line exceeds accuracy: %g', min_dist)
            return MAXFLOAT
        return self.at_ij(min_ind)


class Horizon3D(PlanarCommonInterface):
    def __init__(self, file_name, object_name="Unnamed Horizon 3D", implementation_class=pangea.dxextractobj.Horizon3DFromDX):
        super().__init__(object_name)
        self.file_name = file_name
        if file_name is None:
            self.implementation = None
            self._geometry = None
            self.times = np.array([]).reshape((0, 0))
            self.n_i, self.n_x = 0, 0
            self.origin = np.array([])
            self.v_i = np.array([])
            self.v_x = np.array([])
            self.norm_v_i = 0.0
            self.norm_v_x = 0.0
        else:
            self.implementation = implementation_class(self.file_name)
            self._geometry = self.implementation.geometry
            self.times = np.array(self.implementation.times).reshape((self._geometry.n_i, self._geometry.n_x))
            self.n_i, self.n_x, self.origin, self.v_i, self.v_x = self._geometry
            self.origin = np.array(self.origin)
            self.v_i = np.array(self.v_i)
            self.v_x = np.array(self.v_x)
            self.norm_v_i = np.linalg.norm(self.v_i)
            self.norm_v_x = np.linalg.norm(self.v_x)

    def _xy_to_inline_xline(self, x, y):
        "Convert coordinates into inline-xline numbers"
        xy = np.array([x, y])
        rel_coords = xy - self.origin
        inl_coord = np.inner(self.v_i, rel_coords) / self.norm_v_i
        xnl_coord = np.inner(self.v_x, rel_coords) / self.norm_v_x
        inl = round(inl_coord / self.norm_v_i)
        xln = round(xnl_coord / self.norm_v_x)
        return (int(inl), int(xln))

    def at_ij(self, i, j=0):
        return self.times[i, j]

    def at_xy(self, x, y, accuracy=1.0):
        return self.at_ij(*self._xy_to_inline_xline(x, y))

    @property
    def geometry(self):
        return self._geometry

class Planar3D(Horizon3D):
    
    def __init__(self, file_name, object_name="Unnamed Planar 3D"):
        super().__init__(file_name, object_name=object_name, implementation_class=pangea.dxextractobj.Planar3DFromDX)
        if self.implementation is None:
            pass
        else:
            self.values = np.array(self.implementation.values).reshape((self._geometry.n_i, self._geometry.n_x))

    def at_ij(self, i, j):
        return PlanarValue(self.times[i, j], self.values[i, j])

class PlanarWriter3D(Planar3D, PlanarWriter):

    def __init__(self, file_name, geometry: Horizon3DGeometry, object_name='Unnamed Planar Writer 3D'):
        super().__init__(file_name=None, object_name=object_name)
        PlanarWriter.__init__(self)
        self._geometry = geometry
        self.n_i, self.n_x, self.origin, self.v_i, self.v_x = geometry
        self.times = pangea.np_utils.make_empty_trace(self.n_i * self.n_x).reshape((self.n_i, self.n_x))
        self.values = pangea.np_utils.make_empty_trace(self.n_i * self.n_x).reshape((self.n_i, self.n_x))
        self.origin = np.array(self.origin)
        self.v_i = np.array(self.v_i)
        self.v_x = np.array(self.v_x)
        self.norm_v_i = np.linalg.norm(self.v_i)
        self.norm_v_x = np.linalg.norm(self.v_x)
        self.implementation = pangea.dxextractobj.Planar3DFromDXWriter(file_name, geometry, 
                self.times, self.values, object_name=object_name)

    def add_point_ij(self, val: PlanarValue, i, j=0):
        if self.is_closed:
            raise RuntimeError('Object is closed')
        self.times[i, j] = val.z
        self.values[i, j] = val.val

    def close(self):
        self.implementation.flush()
        self.is_closed = True
        
if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    LOGGER.info('Doing some tests')
    fname2d = '/Users/efremov/Tmp/SEGY_DX/planar_2d.dx'
    p2 = Planar2D(fname2d)
    LOGGER.debug("2D planar value at 100: %s", p2.at_ij(100))
    fname3d = '/Users/efremov/Tmp/SEGY_DX/planar_3d.dx'
    p3 = Planar3D(fname3d)
    LOGGER.debug("3D planar value at 100, 200: %s", p3.at_ij(100, 200))

    fname2d_out = '/Users/efremov/Tmp/SEGY_DX/QQ_planar_2d.dx'
    fname3d_out = '/Users/efremov/Tmp/SEGY_DX/QQ_planar_3d.dx'

    LOGGER.debug("Writing 2D planar to %s ", fname2d_out)
    p2_out = PlanarWriter2D(fname2d_out, p2.xy)
    for i in range(len(p2.xy)):
        p2_out.add_point_ij(p2.at_ij(i), i)
    p2_out.close()
    try:
        p2_out.add_point_ij((2000, 3.14), i)
    except RuntimeError as ex:
        LOGGER.debug("Exception is expected: %s", ex)
    
    p3_out = PlanarWriter3D(fname3d_out, p3.geometry, object_name='My new planar 3d!')
    for i in range(p3.n_i):
        for j in range(p3.n_x):
            p3_out.add_point_ij(p3.at_ij(i, j), i, j)
    p3_out.close()

    p3_in = Planar3D(fname3d_out)
    LOGGER.debug("3D COPY planar value at 100, 200: %s", p3_in.at_ij(100, 200))