from typing import Optional
from fastapi import Depends, Header, HTTPException
import logging
from .db_internals import pool

log = logging.getLogger(__name__)

def extract_name_from_header(x_pangea_user: Optional[str] = Header(None)):
    log.info('Pangea user: %s', x_pangea_user)
    if x_pangea_user is None:
        raise HTTPException(status_code=401, detail='Header bearing user name is required')
    return x_pangea_user

async def get_connection(user=Depends(extract_name_from_header)):
    cn = pool.get_connection()
    cn.set_user(user)
    try:
        yield cn
    finally:
        pool.return_connection(cn)
