from typing import Optional
from fastapi import FastAPI, Depends, Header, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.gzip import GZipMiddleware
from .routers import misc, wells, maps
import logging
from .dependencies import get_connection
from .db_internals.p4dbexceptions import DBAuthoritiesException, DBNotFoundException
from . import settings

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: [%(asctime)s] %(message)s')
log = logging.getLogger(__name__)

app = FastAPI(title="ReView Data Access Server")
if settings.ENABLE_GZIP:
    app.add_middleware(GZipMiddleware, minimum_size=settings.GZIP_MINIMUM_SIZE)
    log.info('Gzip compression enabled for size > %s', settings.GZIP_MINIMUM_SIZE)

@app.exception_handler(DBAuthoritiesException)
def handle_auth_exception(req: Request, ex: DBAuthoritiesException):
    return JSONResponse(status_code=400, content=ex.cause)

@app.exception_handler(DBNotFoundException)
def handle_auth_exception(req: Request, ex: DBNotFoundException):
    return JSONResponse(status_code=400, content=ex.cause)

app.include_router(misc.router, prefix='/aux')
app.include_router(wells.router, prefix='/wells')
app.include_router(maps.router, prefix='/maps')


@app.get('/users')
async def users(cn=Depends(get_connection)):
    log.info('Users requested')
    with cn.getConnection().cursor() as c:
        c.execute('select * from Users')
        res = c.fetchall()
    return res

