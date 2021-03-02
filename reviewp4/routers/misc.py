from fastapi import APIRouter, Depends, Header, Request, Query
from fastapi.responses import StreamingResponse
import logging
import time
import base64
import tempfile
from typing import Optional

import random
import struct
import os
import msgpack

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


async def random_stream_d(n: int, chunks: int):
    cur_n = 0
    chunk_sz = n // chunks
    if chunk_sz < 1:
        log.error('Wrong number of chunks causing chunk size <= 0, setting chunk size to 1')
        chunk_sz = 1
    chunk_data = b''.join([struct.pack('<d', random.random()) for _ in range(chunk_sz)])
    while cur_n < n:
        cur_n += chunk_sz
        yield chunk_data

@router.get('/randomd')
async def randomd(n:int=Query(..., ge=8, description='Total number of doubles to output'), 
                nchunks: Optional[int]=Query(1, ge=1, description='Number of chunks')):
    """Outputs stream of n doubles (LSB) in chunks. Number of chunks is specified in
    the nchunks parameter. Size of double is 8, so, to output 1GB of data by chunks of 10M, n should be 125_000_000,
    number of chunks (nchunks) is 100.
    """
    log.info('Outputting random doubles, n=%d chunk number=%d', n, nchunks)
    return StreamingResponse(random_stream_d(n, nchunks), media_type='application/octet-stream')

async def random_stream_b(sz: int, nchunks: int):
    buf = os.urandom(sz)
    for _ in range(nchunks):
        yield buf

@router.get('/randomb')
async def randomb(sz:int=Query(..., ge=1, description='Size of one chunk'), 
                nchunks: Optional[int]=Query(1, ge=1, description='Number of chunks to output'), 
                cn=Depends(get_connection)):
    """Outputs stream of byte buffers (chunks), each chunk is sz bytes long.
    Total of nchunks is output, i.e. the final data size is sz*nchunks.
    Actually, only the first chunk is generated using random numbers generator, subsequent
    chunks are copies of the first one.
    E.g.: /randomb?sz=10000000&nchunks=100 generates (approx.) 1G of data.
    Note, that generating large dataset in one chunk may be inefficient, random
    numbers generation takes substantial resources.
    """
    log.info('Outputting random bytes, chunk size %s, chunks number=%d', sz, nchunks)
    return StreamingResponse(random_stream_b(sz, nchunks), media_type='application/octet-stream')

async def random_stream_msg(sz: int, nmsgs: int, allrandom: bool, delimit: bool=False):
    buf = os.urandom(sz)
    magic = b'msg1'
    for _ in range(nmsgs):
        msg = msgpack.dumps(buf)
        if delimit:
            msg = magic + struct.pack('<i', len(msg)) + msg
        yield msg
        if allrandom:
            buf = os.urandom(sz)

@router.get('/randommsg')
async def randommsg(sz:int=Query(..., ge=1, description='Size of one chunk'), 
                nmsgs: Optional[int]=Query(1, ge=1, description='Number of chunks to output'), 
                allrandom: Optional[bool] = Query(False, description='Make all messages random'),
                delimit: Optional[bool] = Query(True, description='Add delimiters between messages (b"msg1" + uint32)'),
                cn=Depends(get_connection)):
    """Outputs stream of messages, each message is a byte array (size is sz) of random bytes packed with MessagePack.
    Total of nmsgs is output. Messages are generated independently, that may take some additional time.
    """
    log.info('Outputting random messages, message size %s, messages number=%d, all mesages are random: %s, delimiters: %s', sz, nmsgs, allrandom, delimit)
    return StreamingResponse(random_stream_msg(sz, nmsgs, allrandom, delimit=delimit), media_type='application/octet-stream')
