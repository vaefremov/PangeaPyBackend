from fastapi import APIRouter, Depends, Header, Request, Response, Query
import logging

from reviewp4.utilities.gen_utils import _createOrGetGeologicalObjects
import reviewp4.db_internals.p4dbexceptions as p4dbexceptions
import reviewp4.models as models
import pangea

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

@router.get('/list_maps/{project_name}/{grid_name:path}')
async def list_maps(project_name: str, grid_name:str, db = Depends(get_connection)):
    log.info('Listing maps on grid %s', grid_name)
    prid = db.getProjectByName(project_name)
    mid = db.getContainerByName(prid, None, grid_name)
    subconts = db.getSubContainersListWithCAttribute(mid, 'Path')
    ans = [ s[2] for s in subconts if s[1] == 'grd2']
    return ans
