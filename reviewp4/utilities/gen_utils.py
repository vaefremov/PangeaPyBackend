# General purpose utilities

import reviewp4.db_internals.p4dbexceptions as p4dbexceptions
import logging
import msgpack
import struct
import os

MSG_MAGIC = b'msg1'

log = logging.getLogger(__name__)

# Utilities functions to move elsewhere
def _createOrGetGeologicalObjects(db, prid):
    """Return ID of geo1 container. Create one if it is missing.
    """
    try:
        geo_id = db.getContainerByName(prid, 'geo1', 'Geological Objects')
    except p4dbexceptions.DBException:
        geo_id = db.createContainer(prid, 'geo1', 'Geological Objects')
        log.warning("Geological Objects container created")
    try:
        sid = db.getContainerByName(geo_id, 'str1', 'default')
    except p4dbexceptions.DBException:
        geo_id = db.createContainer(geo_id, 'str1', 'default')
        log.warning("Stratigraphy Objects container created")
    return geo_id

def _createOrGetMetaInf(db, prid):
    """Returns ID of META-INF object. Create it if it does not exist.
    Creates corresponding catalog.
    Return:
        Tuple (meta_id, relative_path_to_META-catalog, absolute_path_to_META-catalog)
    """
    projRoot = '/opt/PANGmisc/DB_ROOT/PROJECTS'
    try:
        mid = db.getContainerByName(prid, 'meta', 'META-INF')
    except p4dbexceptions.DBException as e:
        project_name = db.getContainerName(prid)[1]
        log.error( 'no META-INF for project %s (%s), creating...', project_name, e)
        mid = db.createContainer(prid, 'meta', 'META-INF')
    try:
        m_path = db.getContainerSingleAttribute(mid, 'Path')
        m_abs_path = os.path.join(projRoot, m_path)
    except p4dbexceptions.DBException as e:
        project_name = db.getContainerName(prid)[1]
        log.error( 'no META-INF catalog for project %s (%s), creating...', project_name, e)
        p_path = db.getContainerSingleAttribute(prid, 'Path')
        m_path = os.path.join(p_path, 'META-INF')
        m_abs_path = os.path.join(projRoot, m_path)
        if not os.path.isdir(m_abs_path):
            os.mkdir(m_abs_path)
        db.setContainerSingleAttribute(mid, 'Path', m_path)
    return (mid, m_path, m_abs_path)


def pack_message(obj, add_header: bool = False):
    """Pack message with msgpack and add header for streaming purposes.
    """
    res = msgpack.packb(obj)
    if add_header:
        res = MSG_MAGIC + struct.pack('<i', len(res)) + res
    return res
