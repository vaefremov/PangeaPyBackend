#!/opt/PANGmisc/bin/python
# -*- coding: utf-8 -*-
# $Id: well_utils.py 10633 2009-11-30 16:28:58Z efremov $
"""Utilities to handle data in wells.
"""

__version__ = "$Revision: 10633 $"[11:-2]  # code revision


import struct
import codecs
import pangea.dxextractobj
import pangea.misc_util
import os
import pickle
import types
import datetime, time
import math

import xml.sax
import xml.sax.handler
import xml.sax.xmlreader
import re
from io import StringIO
from gzip import GzipFile
from xmlrpc.client import Binary
from xmlrpc.client import DateTime

import base64
import logging

from reviewp4.db_internals.p4dbexceptions import DBException

log = logging.getLogger(__name__)

MAXFLOAT = 3.40282347e+38 ## stands for undefined values of parameters
MAXFLOAT09 = MAXFLOAT*0.9 ## value to compare to, made less than MAXFLOAT to avoind roud-off errors

def writeCurveData2File(f, data, name):
    """f = open file object. gets closed by this function
    data = curve data [[start, step, encoded_data_float_lsb(<f)], 'curve']
    """
    assert (data[1] == 'curve'), 'Illegal data type %s when calling writeCurveData2File' % data[1]
    header_tmpl = """object 1 class gridpositions counts %d
 origin          %g
 delta           %g
attribute "dep" string "positions"
#
object 2 class gridconnections counts %d
attribute "element type" string "lines"
attribute "dep" string "connections"
attribute "ref" string "positions"
#
object 3 class array type float rank 0 items %d lsb ieee data 0
attribute "dep" string "positions"
#
object "method" class field
component "positions" value 1
component "connections" value 2
component "data" value 3
attribute "name" string "%s"
#
end
"""
    bdata = data[0][2]
    start = data[0][0]
    step = data[0][1]
    n_points = len(bdata.data) / 4
    header = header_tmpl % (n_points, start, step, n_points, n_points, codecs.encode(name, 'utf8'))
    f.write(header)
    f.write(bdata.data)
    f.close()

def writeIrregularCurveData2File(f, data, name):
    """f = open file object. gets closed by this function
    data = irregular curve data [[encoded_data_float_lsb(<f)], 'irregular_curve''
    """
    assert (data[1] == 'irregular_curve'), 'Illegal data type %s when calling writeIrregularCurveData2File' % data[1]
    header_tmpl = """object 1 class array type float rank 1 shape 1 items  %d lsb ieee data data 0
attribute "dep" string "positions"
#
object 2 class patharray count %d
attribute "element type" string "lines"
attribute "ref" string "positions"
attribute "dep" string "connections"
#
object 3 class array type float rank 0 items %d lsb ieee data %d
attribute "dep" string "positions"
#
object "method" class field
component "positions" value 1
component "connections" value 2
component "data" value 3
attribute "name" string "%s"
#
end
"""
    bdata = data[0][0]
    n_points = len(bdata.data) / 8
    data_start = n_points * 4
    header = header_tmpl % (n_points, n_points-1, n_points, data_start, codecs.encode(name, 'utf8'))
    f.write(header)
    for i in range(n_points):
        j = i * 8
        f.write(bdata.data[j:j+4])
    for i in range(n_points):
        j = i * 8
        f.write(bdata.data[j+4:j+8])
    f.close()
    

def findCurveDataMinMax(data):
    """Find minimum and maximum values of data.
    Return: tuple (min, max)
    """
    assert (data[1] == 'curve'), 'Illegal data type %s when calling findCurveDataMinMax' % data[1]
    bdata = data[0][2].data
    n_points = len(bdata) / 4
    d = struct.unpack('<%df' % n_points, bdata)
    d = [x for x in d if x < MAXFLOAT09]
    if len(d):
        max_v = max(d)
        min_v = min(d)
    else:
        max_v = MAXFLOAT
        min_v = MAXFLOAT
    return (min_v, max_v)

def findIrregularCurveDataMinMax(data):
    """Find minimum and maximum values of data.
    Return: tuple (min, max)
    """
    assert (data[1] == 'irregular_curve'), 'Illegal data type %s when calling findIrregularCurveDataMinMax' % data[1]
    bdata = data[0][0].data
    n_points = len(bdata) / 4
    d = struct.unpack('<%df' % n_points, bdata)
    d1 = [d[i] for i in range(1, n_points, 2) if d[i] < MAXFLOAT09]
    if d1:
        max_v = max(d1)
        min_v = min(d1)
    else:
        max_v = MAXFLOAT
        min_v = MAXFLOAT
    return (min_v, max_v)

def findCurveTopBott(data):
    """Find top and bottom for curve data.
    Return: [top, bottom]
    """
    assert (data[1] == 'curve'), 'Illegal data type %s when calling findCurveTopBott' % data[1]
    top = data[0][0]
    bdata = data[0][2].data
    n_points = len(bdata) / 4
    bottom = data[0][0] + data[0][1]*n_points
    return [top, bottom]

def findIrregularCurveTopBott(data):
    """Find top and bottom for curve data.
    Return: [top, bottom]
    """
    assert (data[1] == 'irregular_curve'), 'Illegal data type %s when calling findIrregularCurveTopBott' % data[1]
    bdata = data[0][0].data
    n_points = len(bdata) / 8
    top = struct.unpack('<f', bdata[:4])[0]
    bottom = struct.unpack('<f', bdata[(n_points-1)*8:(n_points-1)*8+4])[0]
    return [top, bottom]

def findBoundariesTopBott(data):
    """Support all data types that are represented by list of pairs (md, value)
    """
    assert (data[1] in ['boundary_method']), 'Illegal data type %s when calling findBoudariesTopBott' % data[1]
    if data[0]:
        top = min(data[0])[0]
        bottom = max(data[0])[0]
    else:
       top = MAXFLOAT
       bottom = MAXFLOAT
    return [top, bottom]

def findLayersTopBott(data):
    """Support all data types that are represented by list of triples (md_top, md_bottom, value)
    """
    assert (data[1] in ['layers_method', 'lithology_method', 'saturation_method', 'stratigraphy', 
                        'measurement', 'test_results_method', 'coring_method', "layer_model"]), 'Illegal data type %s when calling findLayersTopBott' % data[1]
    if data[0]:
        top = min(data[0])[0]
        bottom = max([p[1] for p in data[0]])
    else:
       top = MAXFLOAT
       bottom = MAXFLOAT
    return [top, bottom]

def findBoundariesDataMinMax(data):
    """Find min amd max values of given data.
    Return list of two elements: [min, max]
    """
    assert (data[1] in ['boundary_method']), 'Illegal data type %s when calling findBoundariesDataMinMax' % data[1]
    if data[0]:
        amax = max([p[1] for p in data[0]])
        amin = min([p[1] for p in data[0]])
    else:
        amax = MAXFLOAT
        amin = MAXFLOAT
    return [amin, amax]

def findLayersDataMinMax(data):
    """Find min amd max values of given data.
    Return list of two elements: [min, max]
    """
    assert (data[1] in ['measurement', "layer_model"]), 'Illegal data type %s when calling findLayersDataMinMax' % data[1]
    if data[0]:
        amax = max([p[2] for p in data[0]])
        amin = min([p[2] for p in data[0]])
    else:
        amax = MAXFLOAT
        amin = MAXFLOAT
    return [amin, amax]

def readCurveData(filepath):
    """Read data of well log curve (regular curve).
    Return: [start, step, bdata]
    """
    dx = pangea.dxextractobj.DXParser()
    dx.parse(filepath)
    ol = dx.obj_list
    datastart = dx.datastart
    assert (ol[0].get_class() == 'gridpositions'), 'Illegal DX Object class in well curve[0] - must be gridpositions'
    assert (ol[2].get_data_repr() == 'lsb'), 'Illegal DX Object representation: must be lsb'
    [num_points], [start], [step] = ol[0].get_regarray_params()
    with open(filepath, 'rb') as f:
        f.seek(datastart + ol[2].get_data_addr())
        bdata = f.read(ol[2].get_data_length())
        f.close()
        return [start, step, bdata]

def readIrregularCurveData(filepath):
    """Read data of well log curve (regular curve).
    Return: [bdata]
    """
    dx = pangea.dxextractobj.DXParser()
    dx.parse(filepath)
    ol = dx.obj_list
    datastart = dx.datastart
    assert (ol[0].get_class() == 'array'), 'Illegal DX Object class in well curve[0] - must be array'
    assert (ol[0].get_data_repr() == 'lsb'), 'Illegal DX Object representation: must be lsb'
    assert (ol[2].get_data_repr() == 'lsb'), 'Illegal DX Object representation: must be lsb'
    with open(filepath, 'rb') as f:
        f.seek(datastart + ol[0].get_data_addr())
        bdata1 = f.read(ol[0].get_data_length())
        f.seek(datastart + ol[2].get_data_addr())
        bdata2 = f.read(ol[2].get_data_length())
        f.close()
        bdata = bytearray()
        for i in range(ol[0].get_items_no()):
            bdata.extend(bdata1[i*4:i*4+4])
            bdata.extend(bdata2[i*4:i*4+4])
        return  [bdata]

def readSeismicSegmentData(filepath):
    """
    Read sismic segment data from the DX file designated by filepath
    :param filepath:
    :return: seismic_segment data object corresponding to the input format of writeSeismicSegmentData
    """
    dx = pangea.dxextractobj.DXParser()
    dx.parse(filepath)
    ol = dx.obj_list
    datastart = dx.datastart
    assert (ol[0].get_class() == 'array'), 'Illegal DX Object class in seismic segment[0] - must be array'
    assert (ol[0].get_rank_shape() == (1, 3)), 'Illegal rank and shape of element [0]'
    ntraces = ol[0].get_items_no()
    assert (ol[1].get_class() == 'regulararray'), 'Illegal DX Object class in seismic segment[0] - must be array'
    [nsamples], [skip1, skip2, tstart], [skip3, skip4, tstep] = ol[1].get_regarray_params()
    with open(filepath, 'rb') as f:
        f.seek(datastart + ol[0].get_data_addr())
        coords_data = f.read(ol[0].get_data_length())
        assert (ol[3].get_class() == 'array'), 'Illegal DX Object class in seismic segment[3] - must be array'
        data = f.read(ol[3].get_data_length())
        return [tstart, tstep, nsamples, ntraces, coords_data, data]

def readArrayData(filepath):
    """Read data of well log curve (regular curve).
    Return: [start, step, dim, start2, step2, bdata]
    """
    dx = pangea.dxextractobj.DXParser()
    dx.parse(filepath)
    ol = dx.obj_list
    datastart = dx.datastart
    assert (ol[0].get_class() == 'gridpositions'), 'Illegal DX Object class in well curve[0] - must be gridpositions'
    assert (ol[2].get_data_repr() == 'lsb'), 'Illegal DX Object representation: must be lsb'
    [num_points, dim], [start, start2], [step, tmp1], [tmp2, step2] = ol[0].get_regarray_params()
    with open(filepath, 'rb') as f:
        f.seek(datastart + ol[2].get_data_addr())
        bdata = f.read(ol[2].get_data_length())
        f.close()
        return [start, step, dim, start2, step2, bdata]


def readCurveStartStep(filepath):
    """Read attributes (start, step) of curve. This method only needed to get step
    of previously saved curve.
    Return: list [start, step, number-of-points]
    """
    dx = pangea.dxextractobj.DXParser()
    dx.parse(filepath)
    ol = dx.obj_list
    datastart = dx.datastart
    assert (ol[0].get_class() == 'gridpositions'), 'Illegal DX Object class in well curve[0] - must be gridpositions'
    assert (ol[2].get_data_repr() == 'lsb'), 'Illegal DX Object representation: must be lsb'
    [num_points], [start], [step] = ol[0].get_regarray_params()
    return [start, step, num_points]


def guessMethodFormat(seis_type, curr_format):
    "Guess format of the curve from seismic type. Format may be no specified for curves loaded during time-depth conversion"
    if not (curr_format is None):
        return curr_format
    if seis_type == 'LITHOLOGY':
        return 'lithology_method'
    elif seis_type == 'BOUNDARIES':
        return 'boundary_method'
    else:
        return 'curve'

def  createEmptyData(format):
    "Make empty data object of specified data type (format)"
    if format == 'curve':
        return [[0, 0.2, b''], 'curve']
    elif format in ["boundary_method", "layers_method", "lithology_method", 'saturation_method',  
                    'stratigraphy', 'measurement', 'test_results_method', 'coring_method', "layer_model"]:
        return [[], format]
    else:
        raise RuntimeError("Unsupported format to make empty data object: %s" % format)

def makeActivity(serv_instance, db, user, prid, uid, wid, meth_id, history_line):
    """Returns container id for activity
    """
    mid = serv_instance._createOrGetMetaInf(db, prid)[0]
    # History lines may combine several actions
        # create activity
    act1_id = db.createContainer(mid, 'act1', 'Activity#%s' % uid)
    db.setContainerSingleAttribute(act1_id, 'User', user)
    host = os.uname()[1]
    db.setContainerSingleAttribute(act1_id, 'Host', host)
    db.setContainerSingleAttribute(act1_id, 'Start', None) # current time
    db.setContainerSingleAttribute(act1_id, 'End', None) # ended immediately
    short_comment = ''
    in_ids = []
    hl_split = history_line.split('\t')
    if len(hl_split) == 1:
        a_type = 'Inherited'
        short_comment =  hl_split[0]
    else:
        a_type = hl_split[0] or 'Save log method data'
    program = 'MultiLog'
    my_type = a_type + '/' + program
    if len(hl_split) > 2:
        short_comment += hl_split[1] + ' '
    # try to get input methods:
    if len(hl_split) > 3:
        in_meth_names = hl_split[3].split('&')
        for nm in in_meth_names:
            try:
                in_ids.append(db.getContainerByName(wid, None, nm))  # Here container type was weld
            except:
                pass
    db.setContainerSingleAttribute(act1_id, 'Type', my_type)
    if in_ids:
        db.setContainerSingleAttribute(act1_id, 'InDataSet', in_ids)
    # output method is always current method:
    if type(meth_id) != list:
        db.setContainerSingleAttribute(act1_id, 'OutDataSet', [meth_id])
    else:
        db.setContainerSingleAttribute(act1_id, 'OutDataSet', meth_id)
    db.setContainerSingleAttribute(act1_id, 'ShortComment', short_comment)
    return act1_id

def makeUniqueNames(names_in, names_full, suff):
    """ Make set of names from names in names_in so that they are unique among names. 
    Unique names are done as initial 'name suffN', where N is a number. """
    def checkUniqueBaseNames(cur_names):
        tmpdict = {}
        for n in cur_names:
            if n in tmpdict:
                raise RuntimeError('Non-unique base name %s' % n)
            else:
                tmpdict[n] = 1

    cur_nsuff = []
    cur_names = []
    # escape special chars:
    suff_esc = suff.replace('.', '\.').replace(')', '\)').replace('(', '\(')
    has_fin_bracket = False
    trailing_symb = ''
    patt = "^(.*) %s(\d*)$" % suff_esc
    if suff[-1] == ')':
        has_fin_bracket = True
        patt = "^(.*) %s(\d*)\)$" % suff_esc[:-2]
        trailing_symb = ')'
    rec = re.compile(patt)
    for n in names_full:
        m_obj = rec.match(n)
        if m_obj:
            nn = int(m_obj.group(2) or 0)
            cur_nsuff.append(nn)
        else:
            cur_nsuff.append(None)
    for n in names_in:
        m_obj = rec.match(n)
        if m_obj:
            cur_names.append(m_obj.group(1))
            nn = int(m_obj.group(2) or 0)
            cur_nsuff.append(nn)
        else:
            cur_names.append(n)
            cur_nsuff.append(None)
    checkUniqueBaseNames(cur_names)
    maxnum = max(cur_nsuff)
    if maxnum is None:
        if has_fin_bracket:
            newsuff = ' %s%s' % (suff[:-1], trailing_symb)
        else:
            newsuff = ' %s%s' % (suff, trailing_symb)
    else:
        if has_fin_bracket:
            newsuff = ' %s%d%s' % (suff[:-1], (maxnum + 1), trailing_symb)
        else:
            newsuff = ' %s%d%s' % (suff, (maxnum + 1), trailing_symb)
    names_out = [n + newsuff for n in  cur_names]
    return names_out

def storeMethodData2Db(serv_instance, db,  well_name, w_path, method_name, mid, w_abspath, data, uid):
    assert ( not(data[1] in ["boundary_method"]) ), "This method is supported elsewere (e.g. storeBoundariesData2Db)"
    if data[1] == 'curve':
        fname = well_name + '_D_' + method_name + '_qn.dx'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        writeCurveData2File(f, data, method_name)
        min_v, max_v = findCurveDataMinMax(data)
        top, bottom = findCurveTopBott(data)
        db.setContainerSingleAttribute(mid, 'min', min_v)
        db.setContainerSingleAttribute(mid, 'max', max_v)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'format', 'curve')
        db.setContainerSingleAttribute(mid, 'Type', 'UNKNOWN')
    elif data[1] == 'irregular_curve':
        fname = well_name + '_D_' + method_name + '_qn.dx'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        writeIrregularCurveData2File(f, data, method_name)
        min_v, max_v = findIrregularCurveDataMinMax(data)
        top, bottom = findIrregularCurveTopBott(data)
        db.setContainerSingleAttribute(mid, 'min', min_v)
        db.setContainerSingleAttribute(mid, 'max', max_v)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'format', 'irregular_curve')
        db.setContainerSingleAttribute(mid, 'Type', 'UNKNOWN')
    elif data[1] == "boundary_method": # unsupported in this method
        fname = well_name + '_D_' + method_name + '_bn.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', 'boundary_method')
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findBoundariesTopBott(data)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'Type', 'BOUNDARIES')
    elif data[1] == "layers_method":
        fname = well_name + '_D_' + method_name + '_lr.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', 'layers_method')
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findLayersTopBott(data)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'Type', 'UNKNOWN')
    elif data[1] == "lithology_method":
        fname = well_name + '_D_' + method_name + '_lt.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', 'lithology_method')
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findLayersTopBott(data)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'Type', 'LITHOLOGY')
    elif data[1] == "saturation_method":
        fname = well_name + '_D_' + method_name + '_st.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', 'saturation_method')
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findLayersTopBott(data)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'Type', 'SATURATION')
    elif data[1] == "stratigraphy":
        fname = well_name + '_D_' + method_name + '_sr.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', 'stratigraphy')
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findLayersTopBott(data)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'Type', 'UNKNOWN')
    elif (data[1] == "measurement") or (data[1] == "layer_model") :
        fname = well_name + '_D_' + method_name + '_ms.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', data[1])
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findLayersTopBott(data)  # !!! @TODO: empty dictionary will cause exception!
        amin, amax = findLayersDataMinMax(data)
        # serv_instance.loger.debug('min, max of measurements or layer model', (amin, amax))
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'min', amin)
        db.setContainerSingleAttribute(mid, 'max', amax)
        db.setContainerSingleAttribute(mid, 'Type', 'UNKNOWN')
    elif data[1] == "test_results_method":
        fname = well_name + '_D_' + method_name + '_ts.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', 'test_results_method')
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findLayersTopBott(data)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'Type', 'UNKNOWN')
    elif data[1] == "coring_method":
        fname = well_name + '_D_' + method_name + '_cr.pickled'
        (f, fname) = serv_instance._openUniqFileName(fname, w_abspath, uid)
        db.setContainerSingleAttribute(mid, 'format', 'coring_method')
        pickle.dump(data[0], f)
        f.close()
        top, bottom = findLayersTopBott(data)
        db.setContainerSingleAttribute(mid, 'top', top)
        db.setContainerSingleAttribute(mid, 'bottom', bottom)
        db.setContainerSingleAttribute(mid, 'Type', 'UNKNOWN')
    else:
        raise RuntimeError('Unsupported data type %s' % data[1])

    m_path = os.path.join(w_path, fname)
    db.setContainerSingleAttribute(mid, 'DPath', m_path)
    db.setContainerSingleAttribute(mid, 'ZType', 1)
    try:
        uom = data[2]
        db.setContainerSingleAttribute(mid, 'units', uom)
    except IndexError:
        pass

def readWellMethodDataFromDB(loger, projRoot, db, wid, method_name, encodeb64=True):
    mid = db.getContainerByName(wid, 'weld', method_name)
    try:
        format = db.getContainerSingleAttribute(mid, 'format')
    except DBException as ex:
        log.error('Missing format for method %s, returning empty method: %s', method_name, ex)
        return createEmptyData('curve')
    try:
        path = db.getContainerSingleAttribute(mid, 'DPath')
    except DBException as ex:
        log.error('Invalid or missing DPath attribute: %s', ex)
        ans = createEmptyData(format)
        log.debug('getWellMethodData returns: %s', ans)
        return ans
    abs_path = os.path.join(projRoot, path)
    if format == 'curve':
        ans = readCurveData(abs_path)
        if encodeb64:
            ans[2] = base64.b64encode(ans[2])
        ans = [ans, 'curve']
    elif format == 'array':
        ans = readArrayData(abs_path)
        if encodeb64:
            ans[5] = base64.b64encode(ans[5])
        ans = [ans, 'array']
    elif format == 'seismic_segment':
        tmp = readSeismicSegmentData(abs_path)
        if encodeb64:
            tmp[4] = base64.b64encode(tmp[4]) # coordinates
            tmp[5] = base64.b64encode(tmp[5]) # coordinates
        ans = [tmp, 'seismic_segment']
    elif format in ['irregular_curve', 'reflection_coefficients']:
        bdat = readIrregularCurveData(abs_path)[0]
        if encodeb64:
            bdat = base64.b64encode(bdat)
        ans = [[bdat], format]
    elif (format in ['boundary_method', 'layers_method', 'lithology_method', 'saturation_method',
                     'stratigraphy', 'measurement', 'test_results_method', 'coring_method',
                     'layer_model', 'core_description', 'volume_model']):
        with open(abs_path, 'rb') as f:
            d = pickle.load(f)
            ans = [d,  format]
            f.close()
    else:
        log.error('Unsupported curve format %s for method %s', format,method_name)
        raise RuntimeError(codecs.encode('Unsupported curve format %s' % format, 'utf8'))
    try:
        uom = db.getContainerSingleAttribute(mid, 'units')
    except DBException as ex:
        uom = ''
    ans.append(uom)
    return ans


def storeBoundariesData2Db(self, db, well_name, strid, wid, method_name, mid, data, uid):
    """Boundaries are stored in the special way/
    """
    assert (data[1] == 'boundary_method'), "storeBoundariesData2Db can be used only to store boundaries, not %s" % data[1]
    horsids = {}
    for s in db.getSubContainersListByType(strid,"strb"):
        horsids[s[1].upper()] = s[0]
    # !!! Important note:
    # Here we have to convert horisons names to upper case in order
    # to avoid conflict when the horizon name exists, but cases
    # are not the same. In this case, horsids.has_key will return false,
    # but db.createContainer will cause exception because it 
    # will not let create the horizon with the "same" (by SQL's point of view)
    # name. Hence, usage of upper() method when comparing horizons' names will do the trick!
    boundval = []
    for p in data[0]:
        if p[1].strip().upper() in horsids:
            boundval.append((p[0], horsids[p[1].strip().upper()]))
        else:
            hid = db.createContainer(strid, 'strb', p[1].strip())
            boundval.append((p[0], hid))
    db.setContainerArrayAttribute(mid, 'boundaries', boundval)
    top, bottom = findBoundariesTopBott(data)
    db.setContainerSingleAttribute(mid, 'top', top)
    db.setContainerSingleAttribute(mid, 'bottom', bottom)

def getBoundariesMethodFromDb(db, mid, geoid):
    """Reads boundaries methods from db
    """
    try:
        bnds = db.getContainerArrayAttribute(mid, 'boundaries')
    except:
        bnds = [] 
    data = []
    for p in bnds:
        data.append((p[0], db.getContainerName(p[1])[1] ))
    return [data, "boundary_method"]


def makeCurveDenser(d, EPS):
    """Input is a list of tuples (md, value).
    Output is the same list where coordinates and time are interpolated, value in the closest point is taken as a value
    """
    if len(d) < 2:
        return d
    # Make sure d is sorted properly
    d.sort()
    d_dense = []
    fst_pnt = d[0]
    last_pnt = d[-1]
    md = fst_pnt[0]
    while md < last_pnt[0]:
        d_dense.append((md, pangea.misc_util.valueFromTableNearest(d, md)))
        md += EPS
    return d_dense

def recalculateMDtoTVDbyDL(altitude, dlData, mdList):
    """Accepts directional log data and list of md values, returns TVD (absolute depths).
    """
    
    convTable = [(p['md'], p['tvd'] + 0.0) for p in dlData  ]
    if len(convTable) == 0 or convTable[0][0] != 0.0:
        convTable.insert(0, (0.0, altitude))
    if len(convTable) < 2:
        # Here the case of len(convTable) == 1 means that the only point was added at the 
        # previous step, so it is equal to (0.0, altitude)
        return [altitude - md for md in mdList]
    def findValFromTableExtrapolateLastPoints(md):
        tvd = pangea.misc_util.valueFromTableLinInter(convTable, md)
        if tvd == MAXFLOAT:
            dmd = convTable[-1][0] - convTable[-2][0]
            dmaxmd = md - convTable[-2][0]
            tvd = convTable[-2][1] + (convTable[-1][1] - convTable[-2][1]) * (dmaxmd/dmd)
        elif tvd == -MAXFLOAT:
            return MAXFLOAT
        return tvd
    ans = [findValFromTableExtrapolateLastPoints(md) for md in mdList]
#    return filter(lambda(x): x>=0.0, ans) # Drop all points with md < 0
    return ans

def findValFromTableExtrapolateLastPoints(convTable, md):
    assert len(convTable) >= 2, 'Conversion table too short in well_utils.findValFromTableExtrapolateLastPoints'
    assert type(convTable[0]) == tuple, 'Invalid argument in findValFromTableExtrapolateLastPoints: must be list of tuples'
    tvd = pangea.misc_util.valueFromTableLinInter(convTable, md)
    if tvd == MAXFLOAT:
        dmd = convTable[-1][0] - convTable[-2][0]
        assert abs(dmd) > MD_EPS, 'Distance between two last points of conversion table in well_utils.findValFromTableExtrapolateLastPoints is too small: %f' % dmd
        dmaxmd = md - convTable[-2][0]
        tvd = convTable[-2][1] + (convTable[-1][1] - convTable[-2][1]) * (dmaxmd/dmd)
    elif tvd == -MAXFLOAT:
        return MAXFLOAT
    return tvd

def makeTrajectoryFromDL(maxMD, coords, dlData):
    """Makes trajectory from the directional log data. Extrapolates the trajectory to the maximum
    MD given in the maxMD parameter.
    coords: [x_well, y_well, alt]
    The structure of dlData is as follows:
    [{'md': MD, 'dx': dX, 'dy': dY, 'tvd': TVD}, ...]
    The dlData is assumed to be sorted by md values.
    """
    traj = [(p['dx'] + coords[0], p['dy'] + coords[1], p['tvd'] + 0.0) for p in dlData  ]
    mdList = [p['md'] + 0.0 for p in dlData]
    # Add first point corresponding to collar if md does not start from 0:
    if len(dlData) == 0 or dlData[0]['md'] != 0.0:
        traj.insert(0, coords)
        mdList.insert(0, 0.0)
    if len(traj) < 2:
        traj.append((coords[0], coords[1], coords[2] - maxMD))
    elif (maxMD < MAXFLOAT09) and (maxMD > mdList[-1]):  # do the extrapolation using the last 2 points
        dmd = mdList[-1] - mdList[-2]
        if dmd < MD_EPS:
            raise RuntimeError("Last two points of directional log are too close: %f and %f" % (mdList[-2], mdList[-1]))
        dmaxmd = maxMD - mdList[-2]
        x = traj[-2][0] + (traj[-1][0] - traj[-2][0]) * (dmaxmd/dmd)
        y = traj[-2][1] + (traj[-1][1] - traj[-2][1]) * (dmaxmd/dmd)
        tvd = traj[-2][2] + (traj[-1][2] - traj[-2][2]) * (dmaxmd/dmd)
        traj.append((x, y, tvd))
        mdList.append(maxMD)
    return traj

def getMaxMDForWellFromDB(db, prid, wid):
    minMD, maxMD = db.getMinMaxForParameterInContainer(prid, wid, 'weld', 'bottom')
    minMDb, maxMDb = db.getMinMaxForParameterInContainer(prid, wid, 'wbnd', 'bottom')
    maxMD = max( (maxMD < MAXFLOAT09)*maxMD,  (maxMDb < MAXFLOAT09)*maxMDb )
    return maxMD
    
def adjustTrajectoryDIfNeeded(db_proxy, user, project_name, well_name, db, prid, wid, maxMDwas):
    "Ajust the trajectory in depth scale. Returns True if the trajecotry has been really updated."
    rc = False
    maxMD = getMaxMDForWellFromDB(db, prid, wid)
    try:
        tr_d = db.getContainerArrayAttribute(wid, 'TrajectoryD')
    except p4dbexceptions.DBException as e:
         log.warning("No trajectory in well %s, will create a new one.", well_name)
         tr_d = []
    wx, wy, alt = db.getContainerSingleAttribute(wid, 'Coords');
    if len(tr_d) < 3: # max. 2 points, i.e. strait line, no deviation
        new_tr_d = [(wx, wy, alt), (wx, wy, alt - maxMD)]
        if maxMDwas != maxMD:
            db.setContainerSingleAttribute(wid, 'TrajectoryD', new_tr_d)
            rc = True
            log.warning("Added strait trajectory to well %s", well_name)
            
        ## if len(tr_d) == 2:
        ##     if (alt - tr_d[-1][2]) < maxMD:
        ##         new_tr_d.append((wx, wy, alt - maxMD))  
        ##         db.setContainerSingleAttribute(wid, 'TrajectoryD', new_tr_d)
        ##         rc = True
        ##         db_proxy.loger.warning("Added strait trajectory to well %s" % well_name)
        ## else:
        ##     new_tr_d.append((wx, wy, alt - maxMD))
        ##     db.setContainerSingleAttribute(wid, 'TrajectoryD', new_tr_d)
        ##     rc = True
        ##     db_proxy.loger.warning("Added strait trajectory to well %s" % well_name)
    else:  # There should be directional log data
        if maxMDwas != maxMD:  # maximum MD has changed, need to recalculate trajecory (and possibly the time trajectory)
            log_data = db_proxy.getDirectionalLog(user, project_name, well_name, db = db)
            new_tr_d = makeTrajectoryFromDL(maxMD, (wx, wy, alt), log_data['data'])
            db.setContainerSingleAttribute(wid, 'TrajectoryD', new_tr_d)
            rc = True
            log.warning("Trajectory changed in well %s due to change of max MD from %f to %f",well_name, maxMDwas, maxMD)
    return rc

if __name__ == '__main__':
    from pprint import pprint
    # Do some tests here...
##    fnam = '/nmd/10/efremov/TESTS/WELLS/2-CHG.mlg'
##    fnam = '/nmd/10/efremov/efremov_work/WORK/TEST_DATA/Akhtanizovskaya_1_DD.mlg'
    fnam = '/nmd/10/efremov/efremov_work/WORK/TEST_DATA/TEST_PROJECT/LOG_DATA/MLG_FOR_LOAD/3_en.mlg'
    buff = open(fnam).read()
    res = parseMlgFile(buff)
    print('Well:', res['wellname'])
    print('Coords:', res['coords'])
    tmpl = res['template']
##    for t in tmpl:
##        pprint(t)
    pprint(res['datalist'])
    print()
    print('+++++++++++++++++++++++++++++++++++++++')
    nms = ["aasssaaa", "qqqwwwaaa (init)", "zxczxc zxc zz (init3)"]
    nms1 = ["aasssaaa", "qqqwwwaaa (init)", "zxczxc zxc zz (init3)", 'sdssdf (init10)']
    nms2 = ['asasda', 'qweqweq']
    nms3 = ['(init)', '(init3)']
    print(makeUniqueNames(nms, nms1, "(init)"))
    print(makeUniqueNames(nms2, nms1 + nms2, "(init)"))
    print(makeUniqueNames(nms3, nms1 + nms3, "(init)"))
    nms11 = ["aasssaaa", "qqqwwwaaa (init. )", "zxczxc zxc zz (init. 3)"]
    print(makeUniqueNames(nms11, nms11, "(init. )"))
    nms12 = ["aasssaaa", "qqqwwwaaa init.", "qqqwwwaaa init. ", "zxczxc zxc zz init. 3"]
    print(makeUniqueNames(nms12, nms12, "init. "))
    nms13 = ["aa", "aa (init. )", "zxczxc zxc zz (init. 3)"]
    print(makeUniqueNames(nms11, nms11, "(init. )"))
