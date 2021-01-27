from fastapi import APIRouter, Depends, Header, Request
import logging
import time
import base64
import tempfile
from typing import Optional
from ..dependencies import get_connection, extract_name_from_header

log = logging.getLogger(__name__)

router = APIRouter(tags=['service'])

@router.get('/ping')
async def ping(cn=Depends(get_connection)):
    user = cn.auth.user
    log.info('ping called, user: %s', user)
    # cn.set_user(user)
    return cn.getVersion() + (str(user), cn.auth.getID())

@router.get('/version')
async def version(cn = Depends(get_connection)):
    log.info('Version requested')
    return cn.getVersion()

@router.post('/echo1/{project_name:path}')
async def echo1(project_name: str, req: Request, write2disk: Optional[bool] = False, return_empty_data: Optional[bool] = False):
    t_start = time.time()
    bin_in = await req.body()
    log.info('Echo1: project %s, params %s', project_name, (write2disk, return_empty_data))
    if write2disk:
        with tempfile.TemporaryFile() as f:
            f.write(bin_in)
            f.seek(0)
            bin_out = f.read()
    bin_out = b'' if return_empty_data else bin_in
    bin_out = base64.b64encode(bin_out)        
    return [project_name, len(bin_in), t_start, time.time() - t_start, bin_out]