from typing import Optional
from fastapi import Depends, Header, HTTPException
import logging
from .db_internals import pool, ConnectionContext
from .db_internals.p4dbexceptions import DBAuthoritiesException

log = logging.getLogger(__name__)

def extract_name_from_header(x_diprojects_user: Optional[str] = Header(None)):
    log.info('Pangea user: %s', x_diprojects_user)
    if x_diprojects_user is None:
        raise HTTPException(status_code=401, detail='Header bearing user name is required')
    return x_diprojects_user

async def get_connection(user=Depends(extract_name_from_header)):
    with ConnectionContext() as cn:
        try: 
            cn.set_user(user)
        except DBAuthoritiesException as ex:
            raise HTTPException(status_code=401, detail='Wrong user: ' + ex.cause)
        yield cn
