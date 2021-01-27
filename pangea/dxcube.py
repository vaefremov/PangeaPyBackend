# -*- coding: utf-8 -*-
# $Id: $
""" Module implementing objects used ot read and write 3D seismic objects in the openDX format.
"""

import math
import struct
import numpy as np
import pangea.dxextractobj
import logging


logger = logging.getLogger(__name__)

__version__ = '$Revision:  $'[11:-2]

SAMPLE_BYTE_LEN = 4 # Corresponds to lsb format of data
MAXFLOAT = 3.40282347e+38 ## stands for undefined values of parameters
MAXFLOAT09 = 0.9*3.40282347e+38 ## stands for undefined values of parameters

def scalar_prod(xx, yy):
    return sum(x*y for x, y in zip(xx, yy))

def subtract(x, y):
    return tuple((xx-yy for xx, yy in zip(x, y)))

def add(x, y):
    return tuple((xx+yy for xx, yy in zip(x, y)))

def mult_by_scalar(x, a):
    return tuple((xx*a for xx in x))

def round(x):
    return math.floor(x+0.5)

#######  Utility functions ####

def join_time_axes(t1, t2):
    "Builds the joint time axis from two axes t1 and t2. t1, t2: (t_origin, t_step, n)"
    new_origin = min(t1[0], t2[0])
    new_step = min(t1[1], t2[1])
    # adjust origin, so that it would be multiple of new step
    new_origin = math.ceil(new_origin / new_step) * new_step
    new_n = int(math.floor((max((t1[0] + t1[1]*(t1[2]-1)), (t2[0] + t2[1]*(t2[2])-1)) - new_origin) / new_step)) + 1
    return (new_origin, new_step, new_n)

def recalculate_trace_to_new_time_axis(trace, t1, t_new):
    "Recalculates trace from one time grid to another doing interpolation if needed"
    def trace_value(t):
        ind = (t - t1[0]) / t1[1]
        if (ind < 0) or (ind > t1[2]-1):
            return MAXFLOAT
        alpha, i = math.modf(ind)
        i = int(i)
        if i == t1[2]-1:
            return trace[i]
        if (trace[i] > MAXFLOAT09) or (trace[i+1] > MAXFLOAT09):
            return MAXFLOAT
        return trace[i]*(1.-alpha) + trace[i+1]*alpha
    return tuple(trace_value(t_new[0] + i*t_new[1]) for i in range(t_new[2]))

class DXCube(object):
    "Base class, implements base methods dealing with geometry, etc."
    def __init__(self, object_name=None):
        self.origin = None
        self.v_i = None
        self.v_x = None
        self.n_i = None
        self.n_x = None
        self.n_samples = None
        self.time_step = MAXFLOAT
        self.norm_v_i = None
        self.norm_v_x = None
        self.filename = None
        self.file = None
        self.data_start = None
        self.UNDEF_TRACE = None
        self.object_name = object_name

    def __repr__(self):
        return 'DXCube:{name=' + str(self.object_name) + "}"

    def is_geometry_correct(self):
        ans = isinstance(self.origin, tuple) and (len(self.origin) == 3)
        ans = ans and isinstance(self.v_i, tuple) and (len(self.v_i) == 2)
        ans = ans and isinstance(self.v_x, tuple) and (len(self.v_x) == 2)
        ans = ans and (self.n_i > 0) and (self.n_x > 0)
        return ans

    def close(self):
        if self.file:
            self.file.close()
            self.file = None
            # logger.debug('File %s closed', self.filename)
        return self

    def reopen(self):
        self.file = open(self.filename, 'rb')
        return self

    def geometry(self):
        return (self.origin, self.v_i, self.v_x, self.n_i, self.n_x)

    def time_axis(self):
        return (self.origin[2], self.time_step, self.n_samples)

    def is_valid(self):
        ans = (self.time_step > 0) and (self.n_samples > 0)
        return ans and self.is_geometry_correct()

    def is_point_inside(self, p):
        "Point is 2D"
        assert isinstance(p, tuple) and (len(p) == 2)
        po = subtract(p, self.origin[:2])
        iinl = scalar_prod(po, self.v_i[:2]) / (self.norm_v_i * self.norm_v_i)
        ixl = scalar_prod(po, self.v_x[:2]) / (self.norm_v_x * self.norm_v_x)
        return (iinl >= 0) and (iinl < self.n_i) and (ixl >= 0) and (ixl < self.n_x)

    def set_time_axis(self, start_time, time_step, n_samples):
        self.n_samples = int(n_samples)
        self.origin_time = start_time
        self.origin = self.origin[:2] + (start_time,)
        self.time_step = time_step
        self.UNDEF_TRACE = self.n_samples * (MAXFLOAT,)
        return self

    def set_geometry(self, origin, v_i, v_x, n_i, n_x):
        # logger.debug('origin %s, v_i %s, v_x %s, n_i %s, n_x %s', origin, v_i, v_x, n_i, n_x)
        assert isinstance(origin, tuple) and (len(origin) == 3)
        assert isinstance(v_i, tuple) and (len(v_i) == 2)
        assert isinstance(v_x, tuple) and (len(v_x) == 2)
        assert (n_i > 0) and (n_x > 0)
        # should verify that v_i and v_x are ortogonal and have zero third component
        self.origin = origin
        self.v_i = v_i
        self.v_x = v_x
        self.n_i = int(n_i)
        self.n_x = int(n_x)
        self.norm_v_i = math.hypot(v_i[0], v_i[1])
        self.norm_v_x = math.hypot(v_x[0], v_x[1])
        return self

    def set_geometry_tp(self, geom_tp):
        # logger.debug('geom_tp %s', geom_tp)
        assert isinstance(geom_tp, tuple)
        origin, v_i, v_x, n_i, n_x = geom_tp
        self.set_geometry(origin, v_i, v_x, n_i, n_x)
        return self

    def attach_to_file(self, filename):
        "Get geometry from DX file with the name filename, and set current geometry accordingly"
        dx = pangea.dxextractobj.DXParser()
        dx.parse(filename)
        regarray = None
        for o in dx.obj_list:
            if o.get_class() == 'gridpositions':
                regarray = o.get_regarray_params()
        if regarray is None:
            raise RuntimeError("Invalid input DX file or wrong type: no regular array found")
        # logger.debug("regarray %s", regarray)
        self.set_geometry(tuple(regarray[1]), tuple(regarray[2][:2]), tuple(regarray[3][:2]), regarray[0][0], regarray[0][1])
        self.set_time_axis(-regarray[1][2], -regarray[4][2], regarray[0][2]) # time axis is directed downward
        self.filename = filename
        self.data_start = dx.datastart
        self.reopen()
        assert (dx.data_list[0][1] == 0) # starting address of data
        assert (dx.data_list[0][0].get_data_repr() == 'lsb')  # data format
        return self
    
    def inl_xln_coordinates(self, inl_xln):
        x = self.origin[0] + inl_xln[0] * self.v_i[0] + inl_xln[1] * self.v_x[0]
        y = self.origin[1] + inl_xln[0] * self.v_i[1] + inl_xln[1] * self.v_x[1]
        return x, y

    def traces_numbers_iter(self):
        "Returns iterator, making pairs (inline_no, xline_no) for all traces in cube"
        for inl in range(self.n_i):
            for xln in range(self.n_x):
                yield (inl, xln)

    def traces_coords_iter(self):
        "Returns iterator, making pairs of cooordinates (x, y) for all traces in cube"
        for inl_xln in self.traces_numbers_iter():
            yield self.inl_xln_coordinates(inl_xln)

    def get_trace_by_numbers(self, inl, xln):
        "Return trace as a list of floats corresponding to inl, xln"
        assert (inl >= 0) and (inl < self.n_i)
        assert (xln >= 0) and (xln < self.n_x)
        self.file.seek(self.data_start + (inl*self.n_x + xln)*self.n_samples*SAMPLE_BYTE_LEN)
        buf = self.file.read(self.n_samples*SAMPLE_BYTE_LEN)
        format = '<%df' % self.n_samples
        return struct.unpack(format, buf)

    def get_trace_by_numbers_asarray(self, inl, xln):
        "Return trace as a list of floats corresponding to inl, xln"
        assert (inl >= 0) and (inl < self.n_i)
        assert (xln >= 0) and (xln < self.n_x)
        self.file.seek(self.data_start + (inl*self.n_x + xln)*self.n_samples*SAMPLE_BYTE_LEN)
        buf = self.file.read(self.n_samples*SAMPLE_BYTE_LEN)
        dt = np.dtype('<f')
        return np.fromstring(buf, dtype=dt).astype(np.float64)

    def xy_to_inline_xline(self, x, y):
        "Convert coordinates into inline-xline numbers"
        rel_coords = subtract((x, y), self.origin)
        inl_coord = scalar_prod(self.v_i, rel_coords) / self.norm_v_i
        xln_coord = scalar_prod(self.v_x, rel_coords) / self.norm_v_x
        inl = round(inl_coord / self.norm_v_i)
        xln = round(xln_coord / self.norm_v_x)
        return (int(inl), int(xln))

    def get_nearest_trace_by_coords(self, x, y):
        "Returns the neares trace inside the cube geometry, otherwise return trace filled with MAX_FLOATS"
        inl, xln = self.xy_to_inline_xline(x, y)
        if (inl < 0) or (inl >= self.n_i) or (xln < 0) or (xln >= self.n_x):
            return self.UNDEF_TRACE
        return self.get_trace_by_numbers(inl, xln)

    def corners(self):
        "Returns corners of cube"
        return (self.origin, add(self.origin, mult_by_scalar(self.v_i, self.n_i)),
                add(self.origin, mult_by_scalar(self.v_x, self.n_x)),
                add(self.origin, add(mult_by_scalar(self.v_i, self.n_i), mult_by_scalar(self.v_x, self.n_x))))

    def number_of_traces(self):
        "Returns total number of traces in the cube"
        return self.n_i * self.n_x

    def calculate_wraparound_geometry(self, cubes):
        """Calculate parameters of geometry that is based on current cube (i.e. has the same forming vectors)
        but different origin and numbers of inlines/xlines.
        cubes: list of cubes
        return: (new_origin, v_i, v_x, new_n_i, new_n_x)
        """
        v_min = (0.0, 0.0)
        v_max = (self.norm_v_i * self.n_i, self.norm_v_x * self.n_x)
        for c in cubes:
            delta_o = subtract(c.origin, self.origin)
            delta_i = add(delta_o, mult_by_scalar(c.v_i, c.n_i))
            delta_x = add(delta_o, mult_by_scalar(c.v_x, c.n_x))
            delta_ix = add(delta_o, add(mult_by_scalar(c.v_i, c.n_i), mult_by_scalar(c.v_x, c.n_x)))
            vinl_proj00 = scalar_prod(self.v_i, delta_o) / self.norm_v_i
            vinl_proj10 = scalar_prod(self.v_i, delta_i) / self.norm_v_i
            vinl_proj01 = scalar_prod(self.v_i, delta_x) / self.norm_v_i
            vinl_proj11 = scalar_prod(self.v_i, delta_ix) / self.norm_v_i

            vxl_proj00 = scalar_prod(self.v_x, delta_o) / self.norm_v_x
            vxl_proj10 = scalar_prod(self.v_x, delta_i) / self.norm_v_x
            vxl_proj01 = scalar_prod(self.v_x, delta_x) / self.norm_v_x
            vxl_proj11 = scalar_prod(self.v_x, delta_ix) / self.norm_v_x

            v_min = (min(v_min[0], vinl_proj00, vinl_proj10, vinl_proj01, vinl_proj11), 
                     min(v_min[1], vxl_proj00, vxl_proj10, vxl_proj01, vxl_proj11))
            v_max = (max(v_max[0], vinl_proj00, vinl_proj10, vinl_proj01, vinl_proj11),
                     max(v_max[1], vxl_proj00, vxl_proj10, vxl_proj01, vxl_proj11))
        v_min = (math.floor(v_min[0]/self.norm_v_i) * self.norm_v_i, math.floor(v_min[1]/self.norm_v_x) * self.norm_v_x)
        lowest_corner = add(mult_by_scalar(self.v_i, v_min[0]/self.norm_v_i), mult_by_scalar(self.v_x, v_min[1]/self.norm_v_x))
        new_origin = add(self.origin, lowest_corner)
        new_n_i = math.ceil((v_max[0] - v_min[0]) / self.norm_v_i)
        new_n_x = math.ceil((v_max[1] - v_min[1]) / self.norm_v_x)
        return new_origin, self.v_i, self.v_x, new_n_i, new_n_x


class DXCubeWriter(DXCube):
    def __init__(self, geom_from=None, geom=None, time_axis=None, filename=None, object_name=None):
        super(DXCubeWriter, self).__init__(object_name=object_name)
        if geom_from:
            assert isinstance(geom_from, DXCube)
            geom = geom_from.geometry()
            time_axis = geom_from.time_axis()
        # logger.debug('DXCubeWriter %s, %s, origin=%s', geom, time_axis, geom[0])
        origin = geom[0]
        if len(origin) == 2:
            origin += (0,)
        self.set_geometry_tp(geom)
        self.set_time_axis(time_axis[0], time_axis[1], time_axis[2])
        self.filename = filename
        if self.filename:
            self.open()
        else:
            self.file = None

    def _write_header(self):
        'Writes header to open file'
        hdr_templ = """object 1 class gridpositions counts %d %d %d
origin %.10f %.10f %.3f
delta %.10f %.10f 0
delta %.10f %.10f 0
delta 0 0 %.3f
attribute "dep" string "positions"
#
object 2 class gridconnections counts %d %d %d
attribute "element type" string "cubes"
attribute "dep" string "connections"
attribute "ref" string "positions"
#
object 3 class array type float rank 0 items %d lsb  ieee data 0
attribute "dep" string "positions"
#
object "default" class field
component "positions" value 1
component "connections" value 2
component "data" value 3
attribute "name" string "3D"
#
end
"""
        hdr = hdr_templ % (self.n_i, self.n_x, self.n_samples, 
                           self.origin[0], self.origin[1], -self.origin[2],  
                           self.v_i[0], self.v_i[1], 
                           self.v_x[0], self.v_x[1],
                           -self.time_step,
                           self.n_i, self.n_x, self.n_samples,
                           self.n_i * self.n_x * self.n_samples)
        buf = bytes(hdr, 'utf8')
        self.data_start = len(buf)
        # logger.debug('DEBUG: data_start %s', self.data_start)
        self.file.write(buf)

    def open(self):
        # logger.debug('Openning file %s', self.filename)
        self.file = open(self.filename, 'wb+')
        self._write_header()
        self._write_last_trace()
        return self

    def reopen(self):
        # logger.debug('Reopenning file %s', self.filename)
        self.file = open(self.filename, 'rb+')
        return self

    def write_trace_at_xy(self, trace, x, y):
        inl, xln = self.xy_to_inline_xline(x, y)
        self.write_trace_at_ij(trace, inl, xln)

    def write_empty_trace_at_xy(self, x, y):
        self.write_trace_at_xy(self.UNDEF_TRACE, x, y)

    def write_trace_at_ij(self, trace, inl, xln):
        assert len(trace) == self.n_samples
        assert (inl >= 0) and (inl < self.n_i)
        assert (xln >= 0) and (xln < self.n_x)
        self.file.seek(self.data_start + (inl*self.n_x + xln)*self.n_samples*SAMPLE_BYTE_LEN)
        fmt = '<%df' % self.n_samples
        buf = struct.pack(*(fmt,) + trace)
        self.file.write(buf)

    def write_empty_trace_at_ij(self, inl, xln):
        self.write_trace_at_ij(self.UNDEF_TRACE, inl, xln)

    def np_write_trace_at_ij(self, trace, inl, xln):
        """
        Writes trace to file. Trace is represented as numpy array of doubles.
        :param trace: trace as numpy array
        :param inl: inline number
        :param xln: cross-line number
        :return:
        """
        assert len(trace) == self.n_samples
        assert (inl >= 0) and (inl < self.n_i)
        assert (xln >= 0) and (xln < self.n_x)
        self.file.seek(self.data_start + (inl*self.n_x + xln)*self.n_samples*SAMPLE_BYTE_LEN)
        dt = np.dtype("<f")
        buf = trace.astype(dt).tobytes()
        self.file.write(buf)

    def _write_last_trace(self):
        self.write_empty_trace_at_ij(self.n_i-1, self.n_x-1)

#################### Algorithm of joining cubes
def join_cubes(fname1, fname2, fname_out, messenger = None):
    c1 = DXCube()
    c1.attach_to_file(fname1)
    c2 = DXCube()
    c2.attach_to_file(fname2)
    wr = c1.calculate_wraparound_geometry([c2])
    tj = join_time_axes(c1.time_axis(), c2.time_axis())
    cj = DXCubeWriter(wr, tj, filename=fname_out)
    n_total = 0
    n_from_cubes = 0
    n_final_number_of_traces = cj.number_of_traces()
    for x, y in cj.traces_coords_iter():
        trace_not_yet_written = True
        for c in [c1, c2]:
            if c.is_point_inside((x, y)):
#                print 'DEBUG: will write', x, y, c
                tr = c.get_nearest_trace_by_coords(x, y)
                if tr[0] > MAXFLOAT09:
                    #Undef trace - try another cube.
                    #print 'trace undef', tr[0]
                    continue
                new_tr = recalculate_trace_to_new_time_axis(tr, c.time_axis(), cj.time_axis())
                cj.write_trace_at_xy(new_tr, x, y)
                trace_not_yet_written = False
                n_from_cubes += 1
                break
        #print 'DEBUG: will write empty trace', x, y
        if trace_not_yet_written:
            cj.write_empty_trace_at_xy(x, y)
        n_total += 1
        if messenger:
            messenger.setGauge(n_total, n_final_number_of_traces)
    print('Total number of traces written:', n_total)
    print('Number of points taken from cubes:', n_from_cubes)


def reduce_cube_geometry(fname1, fname2, fname_out, messenger = None):
    c1 = DXCube()
    c1.attach_to_file(fname1)
    c2 = DXCube()
    c2.attach_to_file(fname2)
    wr = c1.geometry()
    tj = c1.time_axis()
    cj = DXCubeWriter(wr, tj, filename=fname_out)
    n_total = 0
    n_from_cubes = 0
    n_final_number_of_traces = cj.number_of_traces()
    for x, y in cj.traces_coords_iter():
        trace_not_yet_written = True
        for c in [c2]:
            if c.is_point_inside((x, y)):
#                print 'DEBUG: will write', x, y, c
                tr = c.get_nearest_trace_by_coords(x, y)
                if tr[0] > MAXFLOAT09:
                    #Undef trace - try another cube.
                    #print 'trace undef', tr[0]
                    continue
                new_tr = recalculate_trace_to_new_time_axis(tr, c.time_axis(), cj.time_axis())
                cj.write_trace_at_xy(new_tr, x, y)
                trace_not_yet_written = False
                n_from_cubes += 1
                break
        #print 'DEBUG: will write empty trace', x, y
        if trace_not_yet_written:
            cj.write_empty_trace_at_xy(x, y)
        n_total += 1
        if messenger:
            messenger.setGauge(n_total, n_final_number_of_traces)
    print('Total number of traces written:', n_total)
    print('Number of points taken from cubes:', n_from_cubes)

if __name__ == '__main__':
    import sys

    # if len(sys.argv) != 4:
    #     print('Usage: dxcube.py input_dxfile_1 input_dxfile_2 output_file')
    #     sys.exit(1)

    #join_cubes("/space6/DB_ROOT/PROJECTS/kurovdag_2013/cube_kurovdag_south_pangea/CenterNorm_tr_qn__raid4_31276.451_1392365391.13.dx", 
    #           "/space6/DB_ROOT/PROJECTS/kurovdag_2013/3D_south/CenterNorm_tr_qn__raid4_31276.451_1392365391.13.dx",
    #           '/space6/DB_ROOT/TMP/Kurovdag_join/out_res.dx')
    #join_cubes(sys.argv[1], sys.argv[2], sys.argv[3])
    # reduce_cube_geometry(sys.argv[1], sys.argv[2], sys.argv[3])
    #
    # sys.exit(0)
#############################################
    c1 = DXCube()
    c1.set_geometry((10, 20, 1700), (0.5, 0), (0, 1.5), 5, 6)
    c1.set_time_axis(1700, 2, 10)
    print(c1.is_geometry_correct())
    print(c1.is_valid())
    print(c1.is_point_inside((0, 0)))
    print(c1.is_point_inside((-1, -2)))
    print(c1.is_point_inside((9, 19)))
    print(c1.is_point_inside((10, 20)))
    print("====== Coordinates of generated cube")
    for x, y in c1.traces_coords_iter():
       print(x, y)
    print("======")

    c4ms = DXCube()
    c4ms.attach_to_file(sys.argv[1])
    print(c4ms.is_valid())
    print(c4ms.is_point_inside((325818., 4425290.)))
    print(c4ms.is_point_inside((327178., 4404887.)))
    print('c4ms geometry:', c4ms.geometry())
    print('c4ms corners:', c4ms.corners())
    print('c4ms time step:', c4ms.time_axis())
    # c2ms = DXCube()
    # c2ms.attach_to_file("DataPart_tr_qn__raid4_3148.24_1391597425.33.dx")
    # print('c2ms geometry:', c2ms.geometry())
    # print('c2ms corners:', c2ms.corners())
    # wr = c4ms.calculate_wraparound_geometry([c2ms])
    # print('Wrap: ', wr)
    # cjoin = DXCube()
    # cjoin.set_geometry(wr[0]+(0,), wr[1], wr[2], wr[3], wr[4])
    # print('Corners of joined cube:', cjoin.corners())
    # print('Time axis c4ms', c4ms.time_axis())
    # print('Time axis c2ms', c2ms.time_axis())
    # print('Time axis cjoin', cjoin.time_axis())
    # tj = join_time_axes(c4ms.time_axis(), c2ms.time_axis())
    # cj = DXCubeWriter(wr, tj, filename='out.dx')
    # cj.write_empty_trace_at_xy(wr[0][0], wr[0][1])
    #
    print('======= reading data')
    # n_inl = 0
    # for i in range(c4ms.n_x):
    #     print('*** ', n_inl, i, c4ms.get_trace_by_numbers(n_inl, i))
    # print('=====*****=====')
    # n_inl = 1
    # for i in range(c4ms.n_x):
    #     print('*** ', n_inl, i, c4ms.get_trace_by_numbers(n_inl, i))
    n_inl = 100
    n_xln = 100
    print("** ", n_inl, n_xln, c4ms.get_trace_by_numbers(n_inl, n_xln))
    print("** ", n_inl, n_xln, c4ms.get_trace_by_numbers_asarray(n_inl, n_xln))

    print('======= writing data')
    filename = "tmp.dx"
    dw = DXCubeWriter(c4ms, filename=filename)
    print(dw.is_valid())
    print(dw.is_point_inside((325818., 4425290.)))
    print(dw.is_point_inside((327178., 4404887.)))
    print('c4ms geometry:', dw.geometry())
    print('c4ms corners:', dw.corners())
    print('c4ms time step:', dw.time_axis())
    dw.write_empty_trace_at_ij(dw.geometry()[3]-1, dw.geometry()[4]-1)

    tr = np.ones(dw.time_axis()[2])
    dw.np_write_trace_at_ij(tr, 0, 0)
    tr_back = dw.get_trace_by_numbers_asarray(0, 0)
    print(np.array_equal(tr, tr_back))

    dx1 = DXCube().attach_to_file(filename)
    tr_back = dw.get_trace_by_numbers_asarray(0, 0)
    print(np.array_equal(tr, tr_back))
    # sys.exit()

    # print('=======')
    # c3 = DXCube()
    # c3.set_geometry((0, 0, 1700), (0., 1.0), (1.0, 0.), 2, 2)
    # print('c3 corners', c3.corners())
    # c4 = DXCube()
    # c4.set_geometry((0, 0, 1700), (2., 0), (0, 2.), 2, 2)
    # print('c4 corners', c4.corners())
    # c5 = DXCube()
    # c5.set_geometry((0, 0, 1700), (1./math.sqrt(2), 1./math.sqrt(2)), (1./math.sqrt(2), -1./math.sqrt(2)), 2, 2)
    # print('c5 corners', c5.corners())
    # wr = c3.calculate_wraparound_geometry([c5])
    # print('Wraparound c5', wr)
    # wr_cube = DXCube()
    # wr_cube.set_geometry(wr[0]+(0,), wr[1], wr[2], wr[3], wr[4])
    # print('wraparound corners:', wr_cube.corners())
    #
    # print('========')
    # x, y = 1.0, 0.0
    # print(x, y, c3.xy_to_inline_xline(x, y))
    # x, y = 1.0, 1.0
    # print(x, y, c3.xy_to_inline_xline(x, y))
    # x, y = 1.5, 0.8
    # print(x, y, c3.xy_to_inline_xline(x, y))
    # x, y = 3, 3
    # print(c1.get_nearest_trace_by_coords(x, y))
    # for i in range(c5.n_i):
    #     for j in range(c5.n_x):
    #         coords = c5.inl_xln_coordinates((i, j))
    #         inl_xln = c5.xy_to_inline_xline(coords[0], coords[1])
    #         print(i, j, inl_xln, (i, j) == inl_xln, coords)
