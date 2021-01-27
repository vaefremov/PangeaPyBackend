# -*- coding: utf-8 -*-
# $Id: $
""" Module implementing objects used ot read and write 2D seismic objects in the openDX format.
"""

import struct
import numpy as np
import pangea.dxextractobj
import logging

logger = logging.getLogger(__name__)

__author__ = 'efremov'

SAMPLE_BYTE_LEN = 4  # Corresponds to lsb format of data
MAXFLOAT = 3.40282347e+38  ## stands for undefined values of parameters
MAXFLOAT09 = 0.9 * 3.40282347e+38  ## stands for undefined values of parameters


class DxLine:
    def __init__(self, object_name=None):
        self.geom = []
        self.data_start = None
        self.origin_time = None
        self.time_step = None
        self.n_traces = None
        self.n_samples = None
        self.UNDEF_TRACE = None
        self.filename = None
        self.file = None
        self.object_name = object_name

    def __repr__(self):
        return 'DXLine:{name=' + str(self.object_name) + "}"

    def set_time_axis(self, start_time, time_step, n_samples):
        self.n_samples = int(n_samples)
        self.origin_time = start_time
        self.time_step = time_step
        self.UNDEF_TRACE = self.n_samples * (MAXFLOAT,)
        return self

    def set_geometry(self, geom):
        self.n_traces = len(geom)
        self.geom = geom

    def geometry(self):
        """
            Returns geometry of the line in the following format: [(x, y, cpd), ...], where
            cpd number starts at 1 and continues with the increment 1.
        """
        return self.geom

    def time_axis(self):
        return self.origin_time, self.time_step, self.n_samples

    def attach_to_file(self, filename):
        "Get geometry from DX file with the name filename, and set current geometry accordingly"
        dx = pangea.dxextractobj.LineGeomFromDX(filename)
        self.set_geometry(dx.getRawGeometry())
        line_params = dx.get_2d_line_params
        # print(line_params)
        self.set_time_axis(-line_params[0][0], -line_params[0][1], line_params[0][2])  # time axis is directed downward
        self.filename = filename
        self.data_start = line_params[2]
        self.reopen()
        # assert (dx.data_list[0][1] == 0) # starting address of data
        # assert (dx.data_list[0][0].get_data_repr() == 'lsb')  # data format
        return self

    def np_get_ith_trace(self, i):
        assert (i >= 0) and (i < self.n_traces)
        self.file.seek(self.data_start + i * self.n_samples * SAMPLE_BYTE_LEN)
        buf = self.file.read(self.n_samples*SAMPLE_BYTE_LEN)
        dt = np.dtype('<f')
        return np.fromstring(buf, dtype=dt).astype(np.float64)

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
            # logger.debug('File %s closed', self.filename)
        return self

    def reopen(self):
        self.file = open(self.filename, 'rb')
        return self

class DXLineWriter(DxLine):
    def __init__(self, geom_from=None, geom=None, time_axis=None, filename=None, object_name=None):
        super(DXLineWriter, self).__init__(object_name=object_name)
        if geom_from:
            assert isinstance(geom_from, DxLine)
            geom = geom_from.geometry()
            time_axis = geom_from.time_axis()
        # logger.debug('DXLineWriter %s ... %s time_axis %s', geom[:4], geom[-4:], time_axis)
        self.set_geometry(geom)
        self.set_time_axis(-time_axis[0], -time_axis[1], time_axis[2]) # time axis goes downward
        self.filename = filename
        if self.filename:
            self.open()
        else:
            self.file = None

    def _write_header(self):
        tmpl = """object 2 class array type float rank 1 shape 3 items  %d lsb  ieee data 0
#
object 3 class regulararray count  %d
origin 0  0 %.3f
delta  0  0 %.3f
#
object 4 class productarray
  term 2
  term 3
attribute "dep" string "positions"
#
object 1 class array type float rank 0 items  %d lsb  ieee data %d
attribute "dep" string "positions"
#
object 5 class gridconnections counts %d %d
attribute "element type" string "quads"
attribute "dep" string "connections"
attribute "ref" string "positions"
object "default" class field
component "positions" value 4
component "connections" value 5
component "data" value 1
attribute "name" string "%s"
#
end
"""
        fmt = '<{n}f{n}f{n}f'.format(n=self.n_traces)
        geom_len = struct.calcsize(fmt)
        hdr = tmpl % (self.n_traces,
                      self.n_samples, self.origin_time, self.time_step,
                      self.n_traces*self.n_samples, geom_len,
                      self.n_traces, self.n_samples,
                      self.object_name)
        buf = bytes(hdr, 'utf8')
        self.data_start = len(buf) + geom_len # Note: data_start refers to start of trace data, skipping geometry
        # logger.debug('DEBUG: data_start %d, geom_len %d', self.data_start, geom_len)
        # self.file.truncate()
        self.file.write(buf)
        self._write_geom()

    def open(self):
        self.file = open(self.filename, 'wb+')
        self._write_header()
        self._write_last_trace()
        # logger.debug("%s open in wb+, cur.addr %d", self.filename, self.file.tell())
        return self

    def reopen(self):
        self.file = open(self.filename, 'rb+')
        # logger.debug("%s open in rb+ mode; datastart=%d, cur.addr %d", self.filename, self.data_start, self.file.tell())
        return self

    def np_write_ith_trace(self, trace, i):
        """
        Writes trace to file. Trace is represented as numpy array of doubles.
        :param trace: trace as numpy array
        :param i: number of trace
        :return:
        """
        assert (i >= 0) and (i < self.n_traces)
        self.file.seek(self.data_start + i * self.n_samples * SAMPLE_BYTE_LEN)
        dt = np.dtype("<f")
        buf = trace.astype(dt).tobytes()
        self.file.write(buf)

    def write_empty_ith_trace(self, i):
        empty = np.array(self.UNDEF_TRACE)
        self.np_write_ith_trace(empty, i)

    def _write_geom(self):
        buf = bytearray()
        for p in self.geom:
            buf.extend(struct.pack('<fff', p[0], p[1], 0.0))
        self.file.write(buf)

    def _write_last_trace(self):
        self.write_empty_ith_trace(self.n_traces-1)
