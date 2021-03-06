from fastapi import APIRouter, Depends, Header, Request, Response, Query
from fastapi.responses import StreamingResponse
import logging
import time
import base64
import tempfile
import os
from typing import Optional, List
import pickle

import reviewp4.utilities.well_utils as well_utils
import reviewp4.utilities.gen_utils as gen_utils
from reviewp4.utilities.gen_utils import _createOrGetGeologicalObjects
import reviewp4.db_internals.p4dbexceptions as p4dbexceptions
import reviewp4.models as models
import pangea

import msgpack

from ..dependencies import get_connection, extract_name_from_header
from ..utilities.gen_utils import pack_message

log = logging.getLogger(__name__)

router = APIRouter(tags=['wells'])

projRoot = '/opt/PANGmisc/DB_ROOT/PROJECTS'
curUidNo = 0  ## Sequential number 
MDT_ACCUR = 0.2 ## Accuracy applied while linearizing md-t correspondence in setMdTimeCorrespondenceForWell.
                ## Depth and time are measured in meters and ms correspondingly, which are of the same order
                ## of magnitude for velocities near 2000 m/s
              
MAXFLOAT = 3.40282347e+38         ## stands for undefined values of parameters
MAXFLOAT09 = 3.40282347e+38 * 0.9 ## a bit less than MAXFLOAT
REALLYBIGTIME = 100000.0          ## 100s of seismic time worth - really big time
MD_EPS = 0.01                     ## Accuracy of MD measurement (used in transforming layer methods to irregular curves)
T_STEP = 0.2    ## Step (along the time axis) used to add points to lithology methods during convertion to time domain (ms)

MULTILOG_PREFIX = "Template#"  # Prefix used to name MultiLog templates
TIE_BINDING_PREFIX = "TieBinding#"  # Prefix used to name WellTie templates
TIE_BINDING_GROUP_PREFIX = "TieBindingGroup#"  # Prefix used to name WellTie templates
HORIZON_PREFIX = "Horizon#"
FAULT_PREFIX = "Fault#"


@router.get('/list/{project_name}')
async def getWells(project_name: str, req: Request, db = Depends(get_connection)):
    """Returns list of wells defined in the project with coordinates.
    Input:
        user - user ID (fake session ID)
        project_name
    Output:
        [(well_name, x, y, altitude), ...]
    """
    def replaceNone(a):
        if a is None:
            return MAXFLOAT
        return a
    log.info('Accepts %s', req.headers.get('accept'))
    prid = db.getProjectByName(project_name)
    tmp = db.getSubContainersListWithAttributesMissingAsNone(prid, 'wel1', ['coords'])
    ans = [(i[1], replaceNone(i[2]), replaceNone(i[3]), replaceNone(i[4])) for i in tmp]
    if req.headers.get('accept') == 'application/octet-stream':
        ansb = msgpack.packb(ans)
        return Response(content=ansb, media_type='application/octet-stream')
    return ans

@router.get('/info/{project_name}')
def getWellsInfo(project_name: str, db = Depends(get_connection)):
    """Returns:
    [{'name': name, 'production_startdate': '...', 'well_type': "producing, injecting, ...", 'field': string, "well_status": string}]
    """
    start_time = time.time()
    prid = db.getProjectByName(project_name)
    wl = db.getSubContainersListByType(prid, 'wel1')
    attrs_l = [ 'production_startdate', 'well_type', 'field', 'well_status', 'pad', 'alias',
                'deposit', 'location', 'province', 'county', 'state', 'country', 'service_company', 'company', 'unique_well_id']
    attrs_def = {'production_startdate': '1900-01-01', 'well_type': '', 'field': '', 'well_status': '', 'pad': '', 'alias': '',
                    'deposit':'', 'location':'', 'province':'', 'county':'', 'state':'', 'country':'', 'service_company':'', 'company':'', 'unique_well_id':''}

    # Retrieve number of methods:
    mn_l = db.countSubContainersByType(prid, 'wel1', ['weld'])
    mn_d = {}
    for m in mn_l:
        mn_d[m[1]] = m[2]
    cl = db.getSubContainersListWithAttributesMissingAsNone(prid, 'wel1', attrs_l)
    ans = []
    for c in cl:
        ans_dict = {'name': c[1]}
        ans_dict['methods_count'] = mn_d.get(c[1]) or 0
        ans_dict.update(attrs_def)
        for i in range(len(attrs_l)):
            ans_dict[attrs_l[i]] = c[i + 2] or attrs_def[attrs_l[i]]
        ans.append(ans_dict)
    log.info("getWellsInfo lasted (s): %s", -start_time + time.time())
    return ans

@router.get('/list_ext/{project_name}')
def getWellsExt(project_name: str, ztype:int, bottom_only: bool=False, db = Depends(get_connection)):
    """Returns list of wells defined in the project with coordinates AND its trajectories
    Input:
        user - user ID (fake session ID)
        project_name
        ztype = 1 for depth
        bottom_only = if true (1) only bottom points are added to trajectory
    Output:
        [(well_name, x, y, altitude, [[x, y, zabs_or_t], ...]), ...]
    """
    def replaceNone(a):
        if a is None:
            return MAXFLOAT
        return a
    start_time = time.time()
    prid = db.getProjectByName(project_name)
    cl = db.getSubContainersListWithAttributesMissingAsNone(prid, 'wel1', ['Coords'])
    if ztype:
        a_name = 'TrajectoryD'
    else:
        a_name =  'TrajectoryT'
    traj_list = db.getSubContainersListWithPVAttribute(prid, 'wel1',  a_name)
    traj_dict = {}
    for p in traj_list: # @TODO: rework!
        if p[0] in traj_dict:
            traj_dict[p[0]].append((p[1], p[2], p[3]))
        else:
            traj_dict[p[0]] = [(p[1], p[2], p[3])]
    ans = []
    for c in cl:
        traj = traj_dict.get(c[1]) or []
        if bottom_only and traj:
            ans.append((c[1], replaceNone(c[2]), replaceNone(c[3]), replaceNone(c[4]),
                        [(replaceNone(c[2]), replaceNone(c[3]), replaceNone(c[4])), traj[-1]]))
        else:
            ans.append((c[1], replaceNone(c[2]), replaceNone(c[3]), replaceNone(c[4]), traj))

    log.info("getWellsExt lasted (s): %s", -start_time + time.time())
    return ans

@router.get('/trajectory/{project_name}/{well_name: path}')
def getWellTrajectory(project_name: str, well_name: str, ztype: int = 0, db = Depends(get_connection)):
    """Return trajectory attribute for given well
    Input:
        user - user ID (fake session ID)
        project_name
        well_name
        ztype = 0 corresponds to time domain, 1 to depth domain.
    """
    prid = db.getProjectByName(project_name)
    wid = db.getContainerByName(prid, None, well_name)
    if ztype:
        traj = db.getContainerArrayAttribute(wid, 'TrajectoryD')
    else:
        traj = db.getContainerArrayAttribute(wid, 'TrajectoryT')
    return traj

@router.get('/directional_log/{project_name}/{well_name:path}')
def getDirectionalLog(project_name: str, well_name: str, req: Request, db = Depends(get_connection)):
    """ Get directional log for well.
    Return:
        The same data structure as was input to getDirectionalLog method.
    """
    prid = db.getProjectByName(project_name)
    wid = db.getContainerByName(prid, 'wel1', well_name)
    dirll = db.getSubContainersListByType(wid, 'dirl')
    if dirll == []:
        return {}
    dirid = dirll[0][0]
    d_path = db.getContainerSingleAttribute(dirid, 'Path')
    d_abspath = os.path.join(projRoot, d_path)
    log.info("Dirlog path: %s", d_abspath)
    with open(d_abspath, 'rb') as f:
        ans = pickle.load(f)
    if req.headers.get('accept') == 'application/octet-stream':
        ansb = msgpack.packb(ans)
        return Response(content=ansb, media_type='application/octet-stream')
    return ans

@router.get('/list_methods/{project_name}/{well_name:path}')
def getWellMethodsList(project_name: str, well_name: str, req: Request, long:bool = True, db = Depends(get_connection)):
    """Return list of dictionaries representing methods data.
    [{"name": "name", "top": double, "bottom": double, "units": "string, uom", "format": "string", "step": double }...]
    """
    def maxfloatIfNone(arg):
        if arg is None:
            return MAXFLOAT
        else:
            return arg

    start_time = time.time()
    prid = db.getProjectByName(project_name)
    wid = db.getContainerByName(prid, 'wel1', well_name)
    m_list = db.getSubContainersListByType(wid,"weld") # here we get list [[id, name], ...]
    if long:
        # Put LAS information into las_dict (
        raw_las_data = db.getAttributeOfSubcontainersByNameOfContainersWhereParentsInSet(prid, [well_name], 'wmif', 'LAS_ID', 'comment')
        las_dict = {}
        for d in raw_las_data:
            tmp = las_dict.get(d[0], {})
            tmp[d[2]] = d[3]
            las_dict[d[0]] = tmp
        ans = []
        for m in m_list:
            d = db.getContainerAttributes(m[0])
            log.debug('Methods attributes: %s', (m, d))
            dout = d
            # well_utils.updateNoUnicode(dout,d)
            dout['name'] = m[1]
            dout['seismic_type'] = d.get('Type') or 'UNKNOWN'
            dout['units'] = d.get('units') or ''
            dout['min'] = maxfloatIfNone(d.get('min'))
            dout['max'] = maxfloatIfNone(d.get('max'))
            dout['top'] = maxfloatIfNone(d.get('top'))
            dout['bottom'] = maxfloatIfNone(d.get('bottom'))
            if dout.get('format'):
                if dout['format'] == 'curve':
                    if 'DPath' in d:
                        fname = os.path.join(projRoot, d['DPath'])
                        step = well_utils.readCurveStartStep(fname)[1] ## !!! not reentrant
                        dout['step'] = step
            dout['las_file'] = las_dict.get(well_name, {}).get(m[1], '')
            ans.append(dout)
    else:
        ans = [{'name': m[1]} for m in m_list]
    # boundaries require special processing:
    m_list = db.getSubContainersListByType(wid,"wbnd") # here we get list [[id, name], ...]
    if long:
        for m in m_list:
            d = db.getContainerAttributes(m[0])
            log.debug('Methods attributes: %s', (m, d))
            dout = d
            # well_utils.updateNoUnicode(dout,d)
            dout['name'] = m[1]
            dout['seismic_type'] =  'BOUNDARIES'
            dout['Type'] =  'BOUNDARIES'
            dout['ZType'] = 1
            dout['format'] = 'boundary_method'
            dout['top'] = maxfloatIfNone(d.get('top'))
            dout['bottom'] = maxfloatIfNone(d.get('bottom'))
            if 'boundaries' in dout:
                del dout['boundaries']
            ans.append(dout)
    else:
        ans +=  [{'name': m[1]} for m in m_list]
    log.debug("getWellMethodsList lasted (s): %s", -start_time + time.time())
    return ans


@router.get('/method_data/{project_name}/{well_name:path}')
def getWellMethodData(project_name: str, well_name: str, req: Request, 
        method_name:str=Query(..., alias='mn', description="method name"), db = Depends(get_connection)):
    """Outputs single well method data. The output may be in json or in msgpack format according to the
    Accept header (application/json or application/octet-stream).
    """
    start_time = time.time()

    prid = db.getProjectByName(project_name)
    wid = db.getContainerByName(prid, 'wel1', well_name)
    output_packed = (req.headers.get('accept') == 'application/octet-stream')
    log.debug('The reply is packed: %s', output_packed)
    try:
        mid = db.getContainerByName(wid, 'wbnd', method_name)
    except p4dbexceptions.DBException:
        log.debug("Well method %s is not boundaries_method", method_name)
    else:
        geoid = _createOrGetGeologicalObjects(db, prid)
        try:
            ans = well_utils.getBoundariesMethodFromDb(db, mid, geoid)
        except p4dbexceptions.DBException:
            log.error("While retrieving method %s from %s", method_name, well_name)
            ans = well_utils.createEmptyData("boundary_method")
        log.debug('getWellMethodData returns: %s', ans)
        log.debug("getWellMethodData lasted (s): %s", -start_time + time.time())
        if output_packed:
            return Response(content=msgpack.packb(ans), media_type='application/octet-stream')
        return ans
    ans = well_utils.readWellMethodDataFromDB(None, projRoot, db, wid, method_name, encodeb64=(not output_packed))
    log.debug('getWellMethodData returns: %s', ans)
    log.debug("getWellMethodData lasted (s): %s", -start_time + time.time())
    if output_packed:
        return Response(content=msgpack.packb(ans), media_type='application/octet-stream')
    return ans

async def methods_data_iter(db, prid: int, well_name: str, wid:int, methods: List[str], delimit: bool = False):
    for method_name in methods:
        log.debug('Outputting method %s', method_name)
        data = well_utils.readWellMethodDataFromDB(None, projRoot, db, wid, method_name, encodeb64=False)
        if data[1] == 'None':
            log.debug('fall back to boundaries')
            data = well_utils.readBoundariesMethodFromDb(db, wid, method_name)
        data = pack_message([well_name, method_name, data], delimit)
        yield data


@router.get('/stream_data/{project_name}/{well_name:path}')
async def streamWellMethodsData(project_name: str, well_name: str,  req: Request, 
        mn: List[str] = Query(..., description='methods names'), 
        delimit: Optional[bool] = Query(True, description='Add delimiters between messages (b"msg1" + uint32)'),
                        db = Depends(get_connection)) -> StreamingResponse:
    """Outputs data of multiple well methods as a stream (sequence) of msgpack messages. Every message has the following format:
    [well_name, method_name, method_data]. This makes the output compatible with stream_multiwell_data.
    """
    log.debug('Stream data params: project %s; well %s; methods %s; add delimiters: %s', project_name, well_name, mn, delimit)
    prid = db.getProjectByName(project_name)
    wid = db.getContainerByName(prid, 'wel1', well_name)
    return StreamingResponse(methods_data_iter(db, prid, well_name, wid, mn, delimit=delimit), media_type='application/octet-stream')

async def multiwell_methods_data_iter(db, prid: int, wells_and_meth: List[models.WellMethodsList], delimit: bool=False):
    for w in wells_and_meth:
        wid = db.getContainerByName(prid, 'wel1', w.well)
        for method_name in w.methods:
            data = well_utils.readWellMethodDataFromDB(None, projRoot, db, wid, method_name, encodeb64=False)
            if data[1] == 'None':
                log.debug('fall back to boundaries')
                data = well_utils.readBoundariesMethodFromDb(db, wid, method_name)
            yield pack_message([w.well, method_name, data], delimit)


@router.post('/stream_multiwell_data/{project_name}')
async def streamWellsMethods(project_name: str, body: List[models.WellMethodsList], 
        delimit: Optional[bool] = Query(True, description='Add delimiters between messages (b"msg1" + uint32)'),
        db = Depends(get_connection)):
    """Outputs log methods data for multiple wells/methods. 
    The output is a sequence (streamed) of 3-element lists in the following format: [well_name, method_name, method_data],
    where method_data coincides with the format output the method_data entry point. Each element if msgpack encoded.
    The input (POST request body) is expected as json in the following format: [{"well": well_name1, "methods": [mn1, mn2, ...]}, ...],
    where mn1, mn2, ... stand for methods names to be output from the well_name1. Eg: 
    [{"well": "11","methods": ["ASP", "IMP_ok 1"]}, {"well": "4", "methods": ["IMP_ok 1","A4M0_5N", 'АК']}]
    """
    log.debug('Multiwell stream: %s, add delimiters: %s', project_name, delimit)
    log.debug('Request body: %s', body)
    prid = db.getProjectByName(project_name)
    return StreamingResponse(multiwell_methods_data_iter(db, prid, body, delimit=delimit), media_type='application/octet-stream')

@router.get('/method_info/{project_name}/{well_name:path}')
def getWellMethodInfo(project_name:str, well_name: str, 
            method_name:str = Query(..., alias='mn', description='method name'), info_name:str = None, db = Depends(get_connection)):
    """Retrieves additional information about log method. If info_name is None, all parameters defined
    for log method mehod_name are returned.
    Return: list [('info_name1', value, units, comment), ...}
    Value type may differ (but currently only strings are returned).
    """
    start_time = time.time()
    assert (pangea.misc_util.stringIsSQLCorrect(method_name)), 'Invalid characters in method name: %s' % method_name
    prid = db.getProjectByName(project_name)
    wid = db.getContainerByName(prid, 'wel1', well_name)
    ans = well_utils.readWellMethodInfo(db, prid, well_name, wid, method_name, info_name)
    log.debug('getWellMethodInfo lasted %s', (time.time() - start_time))
    return ans

def multiwell_methods_info_iter(db, prid: int, wells_and_meth: List[models.WellMethodsList]):
    for w in wells_and_meth:
        wid = db.getContainerByName(prid, 'wel1', w.well)
        for method_name in w.methods:
            yield [w.well, method_name, well_utils.readWellMethodInfo(db, prid, w.well, wid, method_name)]


@router.post('/multiwell_methods_info/{project_name}')
def getMultiwellMethodsInfo(project_name: str, body: List[models.WellMethodsList], db = Depends(get_connection)):
    """Outputs info related to log methods in mutliple wells.
    The output is (JSON) a list of 3-element lists: [[well_name, method_name, info_collection]] where the info_collection
    elements are lists of 4-element lists ('info_name1', value, units, comment) similar to the output of the /wells/method_info
    request.
    """
    prid = db.getProjectByName(project_name)
    return [i for i in multiwell_methods_info_iter(db, prid, body)]

@router.get('/well_profiles_names_and_info/{project_name}')
def getWellProfilesNamesAndInfo(project_name: str, db = Depends(get_connection)):
    """Returns list of names and display information of well profile objects.
    Return:
    [ {'name': string, 'thickness': int, 'color': [int_r, int_g, int_b]} ]
    """
    log.debug("getWellProfilesNames %s", project_name)
    
    prid = db.getProjectByName(project_name)
    mid = gen_utils._createOrGetMetaInf(db, prid)[0]
    pl = db.getSubContainersListByType(mid, 'wprf')
    ans = [{'name':s[1], 
            'thickness': db.getContainerSingleAttributeWithDefault(s[0], 'thickness', 1), 
            'number_of_wells': len(db.getContainerSingleAttributeWithDefault(s[0], 'Refs2wells', [])),
            'color': db.getContainerSingleAttributeWithDefault(s[0], 'color', [0, 0, 0])} 
        for s in pl]
    # ans = [{'name':s[1], 
    #         'thickness': db.getContainerSingleAttributeWithDefault(s[0], 'thickness', 1), 
    #         'color': map(int, db.getContainerSingleAttributeWithDefault(s[0], 'color', [0, 0, 0])),
    #         'number_of_wells': len(db.getContainerSingleAttributeWithDefault(s[0], 'Refs2wells', []))} for s in pl]
    return ans


@router.get('/log_method_names_for_profile/{project_name}')
async def getProjectLogMethodsNames(project_name: str, profile_name: str = Query(..., description="name of wells profile (a named set of wells)"), db = Depends(get_connection)):
    """Return list of well methods defined in project together with additional information,
    such as method type, number of wells that contain this method.
    Return:
        List [ (method_name, format_string, number_of_containing_wells, ...), ... ]
    May rise exceptions.
    """
    start_time = time.time()
    
    log.debug("getProjectLogMethodsNames %s", (project_name, profile_name))
    # now, select boundaries
    prid = db.getProjectByName(project_name)
    mid = gen_utils._createOrGetMetaInf(db, prid)[0]
    wpid = db.getContainerByName(mid, 'wprf', profile_name)
    try:
        wells_refs = db.getContainerArrayAttribute(wpid, 'Refs2wells')
    except p4dbexceptions.DBException as e:
        log.warning('Cannot access well profile %s in project %s ', profile_name, project_name)
        wells_refs = []
    ans = [(d[0].strip(), d[1], d[2]) for d in db.countParentContainersBySubcontainerNameAndAttrConstraintByList(prid, wells_refs, 'wel1', 'weld', 'format')]
    d_bounds = dict(map(lambda p: (p[0].lower().strip(), p[1]), db.countParentContainersBySubcontainerNameConstraintByList(prid, wells_refs, 'wel1', 'wbnd')))
    ans += [(p.strip(), 'boundary_method', d_bounds.get(p.lower().strip()) or 0) for p in db.getDistinctNamesByType(project_name, 'wbnd')]
    log.debug("getProjectLogMethodsNames lasted (s): %s", -start_time + time.time())
    return ans

@router.post('/log_method_names/{project_name}')
async def getProjectLogMethodsNamesForWells(project_name: str, wells_list: List[str], db = Depends(get_connection)):
    """Return list of well methods defined in project together with additional information,
    such as method type, number of wells that contain this method. Accepts JSON list of well names, empty list corresponds to
    all wells in the project.
    Return:
        List [ (method_name, format_string, number_of_containing_wells, ...), ... ]
    May rise exceptions.
    """
    start_time = time.time()
    
    log.debug("getProjectLogMethodsNamesForWells %s", (project_name, wells_list))
    # now, select boundaries
    prid = db.getProjectByName(project_name)
    if len(wells_list) != 0:
        wells_refs = [db.getContainerByName(prid, 'wel1', wn) for wn in wells_list]
        ans = [(d[0].strip(), d[1], d[2]) for d in db.countParentContainersBySubcontainerNameAndAttrConstraintByList(prid, wells_refs, 'wel1', 'weld', 'format')]
        d_bounds = dict(map(lambda p: (p[0].lower().strip(), p[1]), db.countParentContainersBySubcontainerNameConstraintByList(prid, wells_refs, 'wel1', 'wbnd')))
        ans += [(p.strip(), 'boundary_method', d_bounds.get(p.lower().strip()) or 0) for p in db.getDistinctNamesByType(project_name, 'wbnd')]
    else:        
        ans = [(d[0].strip(), d[1], d[2]) for d in db.countParentContainersBySubcontainerNameAndAttr(prid, 'wel1', 'weld', 'format')]
        d_bounds = dict(map(lambda p: (p[0].lower().strip(), p[1]), db.countParentContainersBySubcontainerName(prid, 'wel1', 'wbnd')))
        ans += [(p.strip(), 'boundary_method', d_bounds.get(p.lower().strip()) or 0) for p in db.getDistinctNamesByType(project_name, 'wbnd')]
    log.debug("getProjectLogMethodsNamesForWells lasted (s): %s", -start_time + time.time())
    return ans
