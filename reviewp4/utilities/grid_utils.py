# $Id: grid_utils.py 8224 2009-01-19 09:44:56Z efremov $
"""Utilities to handle grid data.
"""

__version__ = "$Revision: 8224 $"[11:-2]  # code version


import struct
import codecs
import pangea.dxextractobj
import pangea.misc_util
import os
import pickle
import array
import itertools

MAXFLOAT = 3.40282347e+38 ## stands for undefined values of parameters


def getEncodedGridDataFromFile(filepath):
    """Return:
    [[number of points], [origin], [vect2d 1], [vect2d 2], representation_of_data, encoded_data]
    where representation_of_data may by msb or lsb.
    """
    dx = pangea.dxextractobj.DXParser()
    dx.parse(filepath)
    ol = dx.obj_list
    datastart = dx.datastart
    assert (ol[0].get_class() == 'gridpositions'), 'Illegal DX Object class in grid[0] - must be gridpositions'
    num_points, start, step1, step2 = ol[0].get_regarray_params()
    f = open(filepath, 'rb')
    f.seek(datastart + ol[2].get_data_addr())
    bdata = f.read(ol[2].get_data_length())
    f.close()
    repr = ol[2].get_data_repr()
    return [num_points, start, step1, step2, repr, bdata]

def encode_grid(grid_data):
    """Returns a binary representation of grid data. The expected input coinsides with the getEncodedGridDataFromFile output."""
    hdr_format = '<iidddddd'
    return struct.pack(hdr_format, *itertools.chain(*grid_data[:4])) + grid_data[4]


def getGridDataFromFile(filepath):
    """Return:
    [[number of points], [origin], [vect2d 1], [vect2d 2], [data - list of float]]
    where representation_of_data may by msb or lsb.
    """
    num_points, start, step1, step2, repr, bdata = getEncodedGridDataFromFile(filepath)
    n_points = len(bdata)/4
    if repr == 'lsb':
        format = '<%df'
    elif repr == 'msb':
        format = '>%df'
    else:
        raise RuntimeError("Unsupported format of numbers for grid: %s" % repr)
    d = struct.unpack(format % n_points, bdata)
    ans = [num_points, start, step1, step2, d]
    return ans

def calcMaxMinNPoints(d):
    """Calculate max, min and number of valid points in grid data. d is a list of grid points (floats).
    """
    f_data = [x for x in d if x <= 3.40282347e+37]
    if f_data:
        ma = max(f_data)
        mi = min(f_data)
        np = len(f_data)
    else:
        return [MAXFLOAT, MAXFLOAT, 0]
    return [ma, mi, np]


def saveGrid2File(f, data, name):
    """Save grid data to open file.
    data = [[nx, ny], [ox, oy], [dx1, dy1], [dx2, dy2], Binary], where Binary is packed grid data, 4bites float, lsm.
    """
    header_templ = """object 1 class gridpositions counts %d %d
origin %.2f %.2f
delta %.2f %.2f
delta %.2f %.2f
attribute "dep" string "positions"
#
object 2 class gridconnections counts %d %d
attribute "element type" string "quads"
attribute "dep" string "connections"
attribute "ref" string "positions"
#
object 3 class array type float rank 0 items %d lsb ieee data 0
attribute "dep" string "positions"
#
object "" class field
component "positions" value 1
component "connections" value 2
component "data" value 3
attribute "name" string "%s"
#
end
"""
    
    header = header_templ % (data[0][0], data[0][1], 
                             data[1][0], data[1][1], data[2][0], data[2][1], data[3][0], data[3][1],
                             data[0][0], data[0][1], data[0][0] * data[0][1],
                             name)
    f.write(codecs.encode(header, 'utf8'))
    f.write(data[4].data)
    f.close()

######################################################
##   Stuff connected to reading horizons and faults
def read2DHorizonDataFromFile(fname):
    lg = pangea.dxextractobj.Horizon2DFromDX(fname)
    return lg.getRawGeometry()

def read2DHorizonDataFromFileBin(fname):
    "Same as plain readData... but points are encoded in byte array"
    lg = pangea.dxextractobj.Horizon2DFromDX(fname)
    data = lg.getRawGeometry()
    bin_data = array.array('B')
    for p in data:
        bin_data.extend(struct.pack('<3f', p[0], p[1], p[3]))
    return bin_data.tostring()
        
if __name__ == "__main__":
    d = getGridDataFromFile("/opt/PANGmisc/DB_ROOT/PROJECTS/TEST3/map100/map_TJ7503.dx")
    print('DEBUG:', d[0], d[1], d[2], d[3])
    print('DEBUG:', len(d[4]), calcMaxMinNPoints(d[4]))
