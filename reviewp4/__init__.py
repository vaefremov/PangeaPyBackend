version = '0.1.0'
from .main import app
from .db_internals import pool

import logging
logging.basicConfig(level=logging.INFO)