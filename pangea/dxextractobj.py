# -*- coding: utf-8 -*-
# $Id: dxextract.py 6629 2008-06-18 12:56:34Z efremov $
""" Модуль для чтения и анализа объектов, содержащихся в файлах
формата OpenDX. Отличается от dxextract.py тем, что устранены
глобальные переменные.
"""

__version__ = '$Revision: 6629 $'[11:-2]

import re
import struct
import logging
from collections import namedtuple
import pangea.lines_geom

class Horizon3DGeometry(namedtuple('Horizon3DGeometry', 'n_i n_x origin v_i v_x')):
    __slots__ = ()

logger = logging.getLogger(__name__)

##############################################################################
# DXObject
##############################################################################

class DXObject:
    "Class representing object in OpenDX file"
    def __init__(self):
        self.start = -1         # strating byte
        self.end = -1           # next to ending byte
        self.description = ''   # description of object taken from the input file
        self.data = None          # address of data
        self.dxclass = ''       # class of object
        self.id = ''            # object's id
        self.header_line = -1   # No of line header is contained in
        self.data_type = ''     # Type of data part of object
        self.rank = None        # DX rank of object
        self.shape = None       # DX shape of object
        self.items_no = 0       # number of items in object's data
        self.data_repr = None   # Representation of data - e.g. ieee
        self.ext_file = ''      # If nonblank - external file name
        self.ext_file_displ = 0 # Displaccement to data in external file

    def get_description_strings(self):
        """Returns list of strings comprising description of the object"""
        l = self.description.split('\012')
        l = [s.strip() for s in l] #  list(map(string.strip, l))
        # Last element is an empty string
        l.pop()
        # finding header line number
        qry = re.compile('^object\s+')
        for i in range(0, len(l)):
            if qry.match(l[i]):
                self.header_line = i
                break
        return l

    def get_class(self):
        """Returns class of the object"""
        l = self.get_description_strings()
        # Check if there are cashed values:
        if not self.dxclass:
            return self.dxclass
        # This is first invocation of the function, so let us
        # compute rank and shape
        # first - get the description of object
        l = self.get_description_strings()
        h = l[self.header_line]
        # find rank
        mo = re.match('^object\s+.*class\s+(\S+)', h)
        if mo:
            r = mo.group(1)
            self.dxclass = r
        return self.dxclass
        

    def get_component_id(self, str):
        "Returns the ID of component which name is given in argument str"
        l = self.get_description_strings()
        comp = re.compile('^component\s+"' + str + '"\s+value\s+(\w+)')
        # another syntax - without the keyword "value"
        comp1 = re.compile('^component\s+"' + str + '"\s+(\w+)')
        comp_id = None
        for s in l:
            mo = comp.match(s)
            if mo:
                comp_id = mo.group(1)
                break
            # checking against alternative syntax rule...
            mo = comp1.match(s)
            if mo:
                comp_id = mo.group(1)
                break
        return comp_id

    def get_data_addr(self):
        """Returns address of data corresponding to an object.
        None corresponds to no data at all"""
        l = self.get_description_strings()
        # now we have to find string with object's header
        s = l[self.header_line]
        addr = None
        # looking if the external file is present
        qry_ext = re.compile('^object\s+.*data\s+file\s+(\S+)\s*,\s*(\w+)')
        mo = qry_ext.match(s)
        if mo:
            # this is an array object with external file
            self.ext_file = mo.group(1)
            addr = mo.group(2)
            addr = int(addr)
            return addr
        # otherwise - we have internal representation of data
        qry = re.compile('^object\s+.*data\s+(\w+)')
        mo = qry.match(s)
        if mo:
            addr = mo.group(1)
            if addr == 'follows':
                pass
            else:
                try:
                    addr = int(addr)
                except ValueError:
                    addr = None
        return addr

    def get_data_type(self):
        """Returns type of data (for array).
        None is returned if no data are defined."""
        # check for a cached value:
        if self.data_type:
            return self.data_type
        # find data type otherwise
        l = self.get_description_strings()
        h = l[self.header_line]
        mo = re.match('^object\s+.*type\s+(\w+)', h)
        if mo:
            self.data_type = mo.group(1)
        return self.data_type

    def get_items_no(self):
        """Return number of items in data cooresponding to the object.
        0 is returned when there is no data."""
        if self.items_no:
            return self.items_no
        # find number of items
        l = self.get_description_strings()
        h = l[self.header_line]
        mo = re.match('^object\s+.*items\s+(\d+)', h)
        if mo:
            self.items_no = mo.group(1)
            self.items_no = int(self.items_no)
        return self.items_no

    def get_rank_shape(self):
        """Returns tuple containing rank and shape of the object.
        self.rank and self.shape are updated.
        None is returned if object has no shape and rank"""
        # Check if there are cashed values:
        if not self.rank is None:
            return (self.rank, self.shape)
        # This is first invocation of the function, so let us
        # compute rank and shape
        # first - get the description of object
        l = self.get_description_strings()
        h = l[self.header_line]
        # find rank
        mo = re.match('^object\s+.*rank\s+(\d+)', h)
        if mo:
            r = mo.group(1)
            self.rank = int(r)
        # shape makes sense for rank > 0
        if self.rank is not None and self.rank > 0:
            mo = re.match('^object\s+.*shape\s+(\d+)', h)
            if mo:
                s = mo.group(1)
                self.shape = int(s)
        else:
            self.shape = 0
        return (self.rank, self.shape)

    def get_data_repr(self):
        """Gets the representation of data - ascii or ieee
        (or some other...)
        Returns representation string and updates self.data_repr field
        None is returned for inappropriate object class."""
        if self.data_repr:
            return self.data_repr
        # Check for the right object class
        if self.dxclass != 'array' and self.dxclass != 'constantarray':
            return None
        l = self.get_description_strings()
        h = l[self.header_line]
        if re.match('^object.*ieee\s+', h):
            if re.match('^object.*msb\s+', h):
                self.data_repr = 'msb'
            elif re.match('^object.*lsb\s+', h):
                self.data_repr = 'lsb'
            else:
                self.data_repr = 'ieee'
        else:
            # otherwise we suppose that data is in ascii format
            self.data_repr = 'ascii'
        return  self.data_repr

    def get_data_length(self):
        """Returns length of data part corresponding to object's data.
        If the data are ascii returns number of lines with data.
        Returns 0 of there is no data corresponding to object."""
        # length is computed from type of object's data and number of items
        # length is not 0 only for array or constantarray
        length = 0
        if self.dxclass == 'array':
            items = self.get_items_no()
            (r, s) = self.get_rank_shape()
            repr = self.get_data_repr()
            if repr == 'ascii':
                # special case - ascii data
                length = items
            else:
                t = self.get_data_type()
                # compute length of unit item
                if t == 'float':
                  ul = 4
                elif t == 'int':
                    ul = 4
                else:
                  raise 'ERROR: unsuported type %s for object %s' % (t, self.id)
                #  compute length of unit item with rank and shape
                if r > 0:
                  ul = ul * r * s
                length = ul * items
        return length

    def fix_array_description(self):
        """Changes array to constantarray.
        Returns 1 if success, None otherwise"""
        if self.dxclass == 'array':
            self.description = self.description.replace('array', 'constantarray')
            return 1
        return None

    def add_num_attribute(self, attrname, val):
        """Adds numeric attribute to object description"""
        self.description = self.description + "attribute \"" + attrname + "\" number " + str(val) + "\012"

    def add_str_attribute(self, attrname, val):
        """Adds string attribute to object description"""
        self.description = self.description + "attribute \"" + attrname + "\" string \"" + val + "\"\012"

    def get_str_attribute(self, attrname):
        """Gets the value of string attribute of the object.
        Return:
          Attribute value or None if there is no such attribute.
        """
        l = self.get_description_strings()
        for s in l:
            mo = re.match('^attribute\s+\"(\w+)\"\s+string\s+\"([^\"]+)\"', s)
            if mo:
                a = mo.group(1)
                v = mo.group(2)
                if a == attrname:
                    return v
        return None

    def get_regarray_params(self):
        """Get counts, origin and deltas for regular grid.
        Output:
          List [ [nx, ny, ...], [orgx, orgy, ...], [delta1x, delta1y, ...], ...   ]
          None if error occured
        Exceptions:
          ValueError may be raised...
        """
        l = self.get_description_strings()
        h = l[self.header_line]
        # find counts
        mo = re.match('^object\s+.*count[s]{0,1}\s+(\d+.*)', h)
        if mo:
            r = mo.group(1)
            cnts = r.split()
            cnts = list(map(int, cnts))
        else:
            return None
        # here cnts is a list of counts on axes directions
        deltas = []
        orgn = None
        for s in l:
            mo = re.match('^origin\s+([ eE\d\.\-\+]+)', s)
            if mo:
                a = mo.group(1)
                orgn = list(map(float, a.split()))
            mo = re.match('^delta\s+([ eE\d\.\-\+]+)', s)
            if mo:
                a = mo.group(1)
                d = list(map(float, a.split()))
                deltas.append(d)
        if (orgn is None) or (not deltas):
            return None
        ans = [cnts, orgn]
        ans += deltas
        return ans
        
    def get_inline_data(self):
        """This function gets data in the case data are inlined
        inside the object with data follows statement.
        """
        if self.get_data_addr() != 'follows':
            raise RuntimeError('Data are not inlined')
        data = []
        desc_lines = self.description.split('\012')
        for i in range(self.get_data_length()):
            a = desc_lines[i+1].split()
            af = list(map(float, a))
            data += af
        return data
            

    def __str__(self):
        """Converts itself into readable string
        """
        strout = ''
        strout += 'ID: ' + self.id + '\n'
        strout += 'DX class: ' + self.get_class() + '\n'
##        strout += 'ID: ' + self.get_component_id() + '\n'
        strout += 'Data addr: ' + str(self.get_data_addr())  + '\n'
        strout += 'Type: ' + self.get_data_type() + '\n'
        strout += 'Items no: ' + str(self.get_items_no()) + '\n'
        strout += 'Rank/shape: ' + str(self.get_rank_shape()) + '\n'
        strout += 'Data representation: ' + str(self.get_data_repr()) + '\n'
        strout += 'Data length: ' + str(self.get_data_length())
        return strout

##############################################################################
# End DXObject
##############################################################################


##############################################################################
# LineGeomFromDX
##############################################################################

class LineGeomFromDX:
    '''Utility class to extract geometry information from DX files.
    DX files should conform to ReView standards on naming fields
    (field should have name attribute).
    '''
    def __init__(self, f_name):
        dx = DXParser()
        self.f_name = f_name
        dx.parse(f_name)
        self.is_optimized = 0
        # find object field
        nm = None
        for o in dx.obj_list:
            if o.get_class() == 'field':
                nm = o.get_str_attribute('name')
                self.name = nm
        # now find array object having rank 1 shape 3
        items = None
        repr = None
        lengt = None
        dtype = None
        rank = None
        shape = None
        address = None
        obj = None
        self.z_axis = None
        self.n_traces = None
        self.tr_address = None
        self.tr_obj = None
        self.data_start = dx.datastart
        for o in dx.obj_list:
            if o.get_class() == 'array':
                (r, sh) = o.get_rank_shape()
                # print("** r, sh: {}, {}".format(r, sh))
                if r == 1 and sh == 3:
                    # get number of items, address, length and data type
                    items = o.get_items_no()
                    self.n_traces = items
                    repr = o.get_data_repr()
                    lengt = o.get_data_length()
                    dtype = o.get_data_type()
                    address = o.get_data_addr()
                    rank = r
                    shape = sh
                    obj = o # put aside ref to object
                elif r == 0 and sh == 0:
                    tr_items = o.get_items_no()
                    self.tr_address = o.get_data_addr() + dx.datastart
                    # print("Traces:", tr_items, self.tr_address)
                    self.tr_obj = o
                else:
                    continue
            elif o.get_class() == 'regulararray':
                z_coord = o.get_regarray_params()
                self.z_axis = (z_coord[1][2], z_coord[2][2], z_coord[0][0])
                # print("z_coord:", self.z_axis)
            else:
                continue
        # Extract and unpack data
        if address == 'follows':
            data = obj.get_inline_data()
        else:
            format = ''
            if repr == 'lsb':
                format = '<%df' % (shape * items)
            if repr == 'msb':
                format = '>%df' % (shape * items)
            with open(dx.input_file_name, 'rb') as f:
                f.seek(dx.datastart+address)
                sdata = f.read(lengt)
                data = struct.unpack(format, sdata)
        self.geom = []
        self._unpackData(data, items, shape)

    def _unpackData(self, a_data, n_items, a_shape):
        """Unpacks data in internal list of points"""
        for i in range(n_items):
            self.geom.append((a_data[a_shape*i], a_data[a_shape*i+1], i+1))
        
            
    def getRawGeometry(self):
        '''Returns original geometry description of field'''
        return self.geom

    def getGeometry(self):
        '''Returns optimized or raw geometry description of field'''
        if self.is_optimized:
            return self.opt_geom
        else:
            return self.geom
    
    def isOptimized(self):
        return self.is_optimized

    def optimizeGeometry(self, cdp_step = 10):
        self.opt_geom = pangea.lines_geom.opt_geom(self.geom, cdp_step)  # !!!efr - should take real CDP step
        self.is_optimized = 1

    def getName(self):
        return self.name

    @property
    def get_2d_line_params(self):
        return self.z_axis, self.n_traces, self.tr_address

##############################################################################
# End LineGeomFromDX
##############################################################################



##############################################################################
# HorizonFromDX
##############################################################################

class Horizon2DFromDX(LineGeomFromDX):
    '''Utility class to extract horizons from DX files.
    DX files should conform to ReView standards on naming fields
    (field should have name attribute).
    '''
    
    def _unpackData(self, a_data, n_items, a_shape):
        """Unpacks data in internal list of points"""
        for i in range(n_items):
            # if a_data[a_shape*i+2] < 3.4e+38:
            #     # select only points with valid time
            self.geom.append((a_data[a_shape*i], a_data[a_shape*i+1], i+1, a_data[a_shape*i+2]))

##############################################################################
# End HorizonFromDX
##############################################################################


##############################################################################
# 2DPlanar
##############################################################################

class Planar2DFromDX(LineGeomFromDX):
    '''Utility class to read planar data in DX files.
    DX files should conform to ReView standards on naming fields
    (field should have name attribute).
    '''

    def _unpackData(self, a_data, n_items, a_shape):
        """Unpacks data in internal list of points"""
        with open(self.f_name, 'rb') as f:
            f.seek(self.tr_address)
            d = f.read(self.tr_obj.get_items_no()*4)
        fmt = '<%df' % n_items
        if self.tr_obj.get_data_repr() == 'msb':
            fmt = '>%df' % n_items
        dd = struct.unpack(fmt, d)
        for i in range(n_items):
            # if a_data[a_shape*i+2] < 3.4e+38:
            #     # select only points with valid time
            self.geom.append((a_data[a_shape*i], a_data[a_shape*i+1], i+1, dd[i], a_data[a_shape*i+2]))

class Planar2DFromDXWriter:

    def __init__(self, file_name, geometry, times, values, object_name="Unnamed 2D Planar"):
        assert len(geometry) == len(times)
        assert len(geometry) == len(values)
        self.file_name = file_name
        self.geometry = geometry
        self.times = times
        self.values = values
        self.object_name = object_name
        self.n_items = len(geometry)
        self.flush()

    def flush(self):
        hdr_template = """object 1 class array type float rank 0 items {n_items} lsb  ieee data {times_start}
#
object 2 class array type float rank 1 shape 3 items {n_items} lsb  ieee data 0
#
object 3 class path count {n_items}
attribute "element type" string "lines"
attribute "dep" string "connections"
attribute "ref" string "positions"
#
object "data" class field
component "data" value 1
component "positions" value 2
component "connections" value 3
attribute "name" string "{planar_name}"
#
end
"""
        times_start = self.n_items*3*4
        hdr = hdr_template.format(n_items=self.n_items, planar_name=self.object_name, times_start=times_start)
        fmt_coords = '<%df' % (self.n_items*3)
        tmp_geometry_vals = [it for sub in [(i[0][0], i[0][1], i[1]) for i in zip(self.geometry, self.values)] for it in sub]
        buf_geometry_vals = struct.pack(fmt_coords, *tmp_geometry_vals)
        fmt = '<%df' % self.n_items
        buf_times = struct.pack(fmt, *self.times)
        with open(self.file_name, 'wb') as f:
            f.write(hdr.encode())
            f.write(buf_geometry_vals)
            f.write(buf_times)

##############################################################################
# End 2DPlanar
##############################################################################

##############################################################################
# 3DHorizon
##############################################################################

class Horizon3DFromDX:
    '''Utility class to read planar data in DX files.
    DX files should conform to ReView standards on naming fields
    (field should have name attribute).
    '''
    def __init__(self, file_name):
        "Get geometry from DX file with the name filename, and set current geometry accordingly"
        self.file_name = file_name
        dx = pangea.dxextractobj.DXParser()
        dx.parse(file_name)
        regarray = None
        for o in dx.obj_list:
            if o.get_class() == 'gridpositions':
                regarray = o.get_regarray_params()
        if regarray is None:
            raise RuntimeError("Invalid input DX file or wrong type: no regular array found")
        # logger.debug("regarray %s", regarray)

        self.data_start = dx.datastart
        self.times_start = dx.data_list[0][1]
        self.origin = regarray[1]
        self.v_i = regarray[2]
        self.v_x = regarray[3]
        self.n_i, self.n_x = regarray[0]
        self.n_items = self.n_i*self.n_x
        assert dx.data_list[0][1] == 0 # starting address of data
        assert dx.data_list[0][0].get_data_repr() == 'lsb', 'Wrong data format, should be LSB'
        self.times = self.read_data(0, self.n_items)
        self.values = None  # To shut linter up in planars.py

    def read_data(self, addr, n_items):
        """Unpacks data in internal list of points"""
        with open(self.file_name, 'rb') as f:
            f.seek(self.data_start + addr)
            d = f.read(n_items*4)
        fmt = '<%df' % n_items
        # logger.debug('Length of data: %d', len(d))
        return struct.unpack(fmt, d)

    def time_ij(self, i, j):
        return self.times[i*self.n_x + j]

    @property
    def geometry(self):
        return Horizon3DGeometry(n_i=self.n_i, n_x=self.n_x, origin=self.origin, v_i=self.v_i, v_x=self.v_x)

##############################################################################
# End 3DHorizon
##############################################################################

##############################################################################
# 3DPlanar
##############################################################################

class Planar3DFromDX(Horizon3DFromDX):
    '''Utility class to read horizon data in DX files.
    DX files should conform to ReView standards on naming fields
    (field should have name attribute).
    '''
    def __init__(self, file_name):
        "Get geometry from DX file with the name filename, and set current geometry accordingly"
        super(Planar3DFromDX, self).__init__(file_name)
        self.times_start = self.n_items*4
        self.values = self.times
        self.times = self.read_data(self.times_start, self.n_items)

    def value_ij(self, i, j):
        return self.values[i*self.n_x + j]


class Planar3DFromDXWriter:
    def __init__(self, file_name, geom: Horizon3DGeometry, times, values, object_name="Unnamed 3D Planar from DX"):
        self.file_name = file_name
        self.geometry = geom
        self.n_i, self.n_x, self.origin, self.v_i, self.v_x = geom
        self.times = times.reshape(self.n_i * self.n_x)
        self.values = values.reshape(self.n_i * self.n_x)
        assert len(self.times) == self.n_i * self.n_x, 'Lengths not equal %d != %d' % (len(self.times), self.n_i * self.n_x)
        assert len(self.values) == self.n_i * self.n_x
        self.object_name = object_name
        self.flush()

    def flush(self):
        hdr_template = """object 1 class gridpositions counts {n_i} {n_x}
origin {o_x} {o_y}
delta {v_i_x} {v_i_y}
delta {v_x_x} {v_x_y}
attribute "dep" string "positions"
#
object 2 class gridconnections counts {n_i} {n_x}
attribute "element type" string "quads"
attribute "dep" string "connections"
attribute "ref" string "positions"
#
object 3 class array type float rank 0 items 249830 lsb  ieee data 0
attribute "dep" string "positions"
#
object 4 class array type float rank 0 items 249830 lsb  ieee data {times_start}
attribute "dep" string "positions"
#
object "scalar_geometry" class field
component "positions" value 1
component "connections" value 2
component "data" value 4
attribute "name" string "scalar_geometry"
#
object "default" class field
component "positions" value 1
component "connections" value 2
component "data" value 3
attribute "name" string "{planar_name}"
#
end
"""
        n_items = self.n_i * self.n_x
        times_start = n_items * 4
        hdr = hdr_template.format(n_i=self.n_i, n_x=self.n_x,
                        o_x=self.origin[0], o_y=self.origin[1],
                        v_i_x=self.v_i[0], v_i_y=self.v_i[1],
                        v_x_x=self.v_x[0], v_x_y=self.v_x[1], times_start=times_start,
                        planar_name=self.object_name)
        fmt = '<%df' % n_items
        with open(self.file_name, 'wb') as f:
            f.write(hdr.encode())
            f.write(struct.pack(fmt, *self.values))
            f.write(struct.pack(fmt, *self.times))

    def time_ij(self, i, j):
        return self.times[i*self.n_x + j]

    def value_ij(self, i, j):
        return self.values[i*self.n_x + j]

##############################################################################
# End 3DPlanar
##############################################################################




##############################################################################
# WellFromDX
##############################################################################

class WellFromDX(LineGeomFromDX):
    '''Utility class to extract wells from DX files.'''
    
    def _unpackData(self, a_data, n_items, a_shape):
        """Unpacks data in internal list of points"""
        for i in range(n_items):
            if a_data[a_shape*i+2] < 3.4e+38:
                # select only points with valid time
                self.geom.append((a_data[a_shape*i], a_data[a_shape*i+1], a_data[a_shape*i+2]))

    def optimizeGeometry(self, dist = 0):
        self.opt_geom = pangea.lines_geom.opt_geom3(self.geom, dist)  # !!!efr - should take real CDP step
        self.is_optimized = 1

##############################################################################
# End WellFromDX
##############################################################################

##############################################################################
# PArser of dx file: converted from non-reenterant older code
##############################################################################

class DXParser:
    def __init__(self):
        # Global variables
        # name of input file
        self.input_file_name = ''
        # List of objects description parsed in input file
        self.obj_list = []
        # address of data (just after the "end" keyword)
        self.datastart = -1
        # list of triples <object ref, data address, data length>
        self.data_list = []

    def new_object(self, line):
        "Returns matching object or none"
        return re.match('^object', line)

    def get_class(self, line):
        "Extracts DX class of object from string"
        mo = re.match('^object\s+\S+\s+class\s+(\S+)', line)
        if mo is None:
            # try to handle object name containing spaces, inb this case the name will be enclosed in "
            mo = re.match('^object\s+"[^"]+"\s+class\s+(\S+)', line)
        try:
            c = mo.group(1)
        except:
            c = '' # this may be wrong! 
        return c

    def get_id(self, line):
        "Gets the id of object"
        mo =   re.match('^object\s+(\S+)\s', line)
        if mo:
            try:
                s = mo.group(1)
            except:
                s = ''
        return s

    def parse(self, file_name):
        """Parse input file until keyword end happens.
        Build list of objects acqainted in the input file.
        """
        # Reinitialize global vars
        self.input_file_name = file_name
        self.obj_list = []
        self.datastart = -1
        self.data_list = []

        fin = open(file_name, 'rb')

        # making new object = starting with it
        cur = DXObject()
        cur.start = fin.tell()
        prev_tell = fin.tell()
        l = fin.readline().decode('utf-8')

        end = 0
        first_obj = 1
        while not end and l:
            end = (l == 'end\012')
            if not end:
                if self.new_object(l):
                    if not first_obj:
                        # finalizing current object
                        cur.end = prev_tell
                        self.obj_list.append(cur)
                        # starting new object
                        cur = DXObject()
                        cur.start = prev_tell

                    first_obj = 0
                    cur.dxclass = self.get_class(l)
                    cur.id = self.get_id(l)
                cur.description = cur.description + l
                # finding out about data in object
                if self.data_line(l):
                    data_addr = cur.get_data_addr()
                    data_length = cur.get_data_length()
                    self.data_list.append((cur, data_addr, data_length))
                prev_tell = fin.tell()
                l = fin.readline().decode('utf-8')

        # previous read statement was reading of 'end'
        cur.end = prev_tell
        self.obj_list.append(cur)
        # recording starting address of data
        self.datastart = fin.tell()
        fin.close()

    def get_field(self):
        "Returns object corresponding to field in objects list"
        return [o for o in self.obj_list if o.dxclass == "field"]

    def find_by_id(self, id):
        "Finds object by idetificator"
        return list(filter(lambda o, oid = id: o.id == oid, self.obj_list))


    def output_2_file(self, fname):
        "Writes abbridged version of object list to external file"
        # getting main (field) element
        f = self.get_field()[0]
        i = f.get_component_id('data')
        # finding data object
        d = self.find_by_id(i)[0]
        # making data object constantarray
        d.fix_array_description()
        # preparing to output
        # Find data address of data object
        data_address = d.get_data_addr()
        # compute displacement to data in input file
        data_displ = self.datastart + data_address
        # addind description of data in attributes of field
        f.add_num_attribute('PNG_start', data_displ)
        f.add_str_attribute('PNG_byteorder', d.get_data_repr())
        f.add_str_attribute('PNG_binfile', self.input_file_name)

        fout = open(fname, 'wb')
        for o in self.obj_list:
            fout.write(o.description)
        # Adding end keyword
        fout.write('end\012')
        fout_datastart = fout.tell()
        # now - outputting data
        self.fix_data_list()
        # copy all the data except main data object
        fin = open(self.input_file_name, 'rb')
        for i in range(0, len(self.data_list)):
            o = self.data_list[i]
            fin.seek(self.datastart + o[1])
            if o[0] is d:
                # if this is a data object - write just beginning of data
                newlen = o[2]/d.get_items_no()
                # s = fin.read(newlen)
                # Try to handle strange behaviour of dx:
                s = '\000' * newlen
            else:
                # regular object's data
                s = fin.read(o[2])
            # standing at the point where the data should be
            fout.seek(fout_datastart + o[1])
            fout.write(s)
        fin.close()
        fout.close()

    def comment_line(self, str):
        """Returns 1 if str is a comment line"""
        str = str.strip()
        if re.match('^#', str):
            return 1
        return 0

    def data_line(self, str):
        "Defines if current line is a data description."
        return re.match('^object\s+.*data\s+', str)

    def sort_data(self):
        """Sorts the data_obj list in ascending order of data shift"""
        self.data_list.sort(lambda x, y: x[1] - y[1])

    def fix_data_list(self):
        """Fixes (try to) data list items with 0 length"""
        self.sort_data()
        new_list = []
        for i in range(0, len(self.data_list)):
            o = self.data_list[i]
            ln = o[2]
            if o[2] == 0:
                if i == len(self.data_list)-1:
                    # last element - no fixup yet
                    raise RuntimeError("ERROR: last data element with 0 length not implemented")
                onext = self.data_list[i+1]
                ln = onext[1] - o[1]
            # reconstruct element and put to new list
            new_list.append((o[0], o[1], ln))
        # Swap  lists
        self.data_list = new_list



def test(filename = '/home/pangea/efremov/QQQ1/hord.dx'):
    # /home/pangea/efremov/tmp/OpenDX/dxsamples-4.0.8/data/topo_one_deg.dx
    dx = DXParser()
    dx.parse(filename)
    print("Data start: ", str(dx.datastart))
    f = dx.get_field()[0]
    print(f.description)
    i = f.get_component_id('data')
    print("Data ID: ", i)
    # finding data object
    d = dx.find_by_id(i)[0]
    print(d.get_description_strings())
    print("Header no: ", str(d.header_line))
    print("Data address: ", d.get_data_addr())
    print("Data list", dx.data_list)
    print("Data type of data obj", d.get_data_type())
    print("Data list:", dx.data_list)
    for d in dx.data_list:
        print(" ID, data start, length   ", d[0].id, d[1], d[2])
        print(" (rnk, shape), items, repr.  ", d[0].get_rank_shape(), d[0].get_items_no(), d[0].get_data_repr())
    for o in dx.obj_list:
        print('Object:', o)
        print("Class of obj", o.get_class())
        print(' reg. array pars ', o.get_regarray_params())
        
#    print "fixing data list"
#    fix_data_list()
#    print dx.data_list


if __name__ == '__main__':
    import sys
    test(sys.argv[1])
    print('*****************************************')
#    h = HorizonFromDX('/home/pangea/efremov/QQQ1/hord.dx')
    h = Horizon2DFromDX(sys.argv[2])
    h.optimizeGeometry()
    print(h.getGeometry())
    print('******************************************')
#    test('/nmd/10/grivna/DB_ROOT/PROJECTS/UNT_CUBE/unt_cube/Prediction__3574_1044617832.69.dx')
    
