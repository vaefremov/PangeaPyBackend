# General purpose utilities

import reviewp4.db_internals.p4dbexceptions as p4dbexceptions
import logging

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