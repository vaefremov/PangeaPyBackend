from threading import BoundedSemaphore
import logging
import warnings
from .. import settings
import reviewp4.db_internals.p4db as p4db
# logging.basicConfig(level=logging.DEBUG)

log = logging.getLogger(__name__)

try:
    import MySQLdb as mysql_driver
except ModuleNotFoundError:
    log.debug('Using fall-back driver for MySQL')
    import pymysql as mysql_driver # fall-back module

def create_db_connection():
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        c = mysql_driver.connect(host=settings.DB_HOST, user=settings.DB_USER, password=settings.DB_PASSWORD, db=settings.DB_DATABASE, charset='utf8')
    db = p4db.P4DBbase(c)
    return db

class MyConnectionsPool:
    def __init__(self, n_connections):
        self.sema = BoundedSemaphore(n_connections)
        self.connections = set(create_db_connection() for i in range(n_connections))
        log.debug('Created connections pool: %d', len(self.connections))

    def get_connection(self) -> p4db.P4DBbase:
        log.debug('Getting connection to DB %s as %s', settings.DB_HOST, settings.DB_USER)
        self.sema.acquire()
        c = self.connections.pop()
        log.debug('Connections left (get): %d', len(self.connections))
        
        c.connection.ping(True)
        return c

    def return_connection(self, c):
        self.connections.add(c)
        self.sema.release()
        log.debug('Connections left (return): %d', len(self.connections))

pool = MyConnectionsPool(settings.DB_POOL_SZ)