from fastapi import APIRouter, Depends, Header, Request, Response, Query
from fastapi.responses import StreamingResponse
import math
import os
import base64
import logging

from reviewp4.utilities.gen_utils import _createOrGetGeologicalObjects
import reviewp4.db_internals.p4dbexceptions as p4dbexceptions
import reviewp4.models as models
import pangea
import reviewp4.utilities.grid_utils as grid_utils

from ..dependencies import get_connection, extract_name_from_header
from ..utilities.gen_utils import pack_message

log = logging.getLogger(__name__)

router = APIRouter(tags=['maps'])

projRoot = '/opt/PANGmisc/DB_ROOT/PROJECTS'

@router.get('/list/{project_name}')
async def list_maps(project_name: str, db = Depends(get_connection)):
    """Returns list of grid geometries defined in the project specified with the project_name parameter.
    The result format is similar to the following one:
           Example:
            {
                "maps": [
                    {
                        "name": "MAP",
                        "n_steps": [
                            231,
                            316
                        ],
                        "origin": [
                            13490018.6,
                            6676487.2
                        ],
                        "dx": [
                            86.567613,
                            50.060448
                        ],
                        "dy": [
                            -50.060448,
                            86.567613
                        ]
                    }
                ],
                "project": "test2"
            }        
    """
    prid = db.getProjectByName(project_name)
    tmp = db.getSubContainersListWithAttributesMissingAsNone(prid, 'map', ['Nx', 'Ny', 'Origin', 'Dx', 'Dy'])
    res = []
    for m in tmp:
        cur_map = {}
        cur_map['name'] = m[1]
        cur_map['n_steps'] = [m[2], m[3]]
        cur_map['origin'] = m[4:6]
        cur_map['dx'] = m[7:9]
        cur_map['dy'] = m[10:12]
        res.append(cur_map)
    return {'maps': res, 'project': project_name}

@router.get('/grid_geometry/{project_name}/{grid_name:path}')
async def get_map_geometry(project_name: str, grid_name:str, db = Depends(get_connection)):
    """Returns parameters of a concrete grid
    """
    prid = db.getProjectByName(project_name)
    cid = db.getContainerByName(prid, 'map', grid_name)
    Nx = db.getContainerSingleAttribute(cid, 'Nx')
    Ny = db.getContainerSingleAttribute(cid, 'Ny')
    orig = db.getContainerSingleAttribute(cid, 'Origin')
    dx = db.getContainerSingleAttribute(cid, 'Dx')
    dy = db.getContainerSingleAttribute(cid, 'Dy')
    ans = {}
    ans['numbers'] = [Nx, Ny]
    ans['origin']= orig[0:2]
    ans['dx'] = dx[0:2]
    ans['dy'] = dy[0:2]
    ans['alpha'] = math.atan2(dx[1], dx[0]) * (180./math.pi)
    ans['step_x'] = math.sqrt(dx[0]*dx[0] + dx[1]*dx[1])
    ans['step_y'] = math.sqrt(dy[0]*dy[0] + dy[1]*dy[1])
    return ans


@router.get('/list_maps/{project_name}/{grid_name:path}')
async def list_maps(project_name: str, grid_name:str, db = Depends(get_connection)):
    """Returns list of maps (grid data) belonging to the specified grid
    """
    log.info('Listing maps on grid %s', grid_name)
    prid = db.getProjectByName(project_name)
    mid = db.getContainerByName(prid, None, grid_name)
    subconts = db.getSubContainersListWithCAttribute(mid, 'Path')
    ans = [ s[2] for s in subconts if s[1] == 'grd2']
    return ans

@router.get('/grid_data/{project_name}/{grid_name:path}')
async def grid_data(project_name: str, grid_name:str, map_name:str, db = Depends(get_connection)):
    """Returns grid data in the following format:
       <iidddddd + data(f4)
    """
    prid = db.getProjectByName(project_name)
    mid = db.getContainerByName(prid, None, grid_name)
    mpath = db.getContainerSingleAttribute(mid, 'Path')
    mpath_abs = os.path.join(projRoot, mpath)
    gid = db.getContainerByName(mid, 'grd2', map_name)
    gpath = db.getContainerSingleAttribute(gid, 'Path')
    gpath_abs = os.path.join(projRoot, gpath)
    log.info('Getting data from file %s', gpath_abs)
    gData = grid_utils.getEncodedGridDataFromFile(gpath_abs)
    assert (gData[4] == 'lsb'), 'Wrong byte order in data, lsb expected, found %s' % gData[4]
    del gData[4]
    tmp_bin = grid_utils.encode_grid(gData)
    return Response(content=tmp_bin, media_type='application/octet-stream')