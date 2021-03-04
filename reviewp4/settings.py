import os
import configparser
import logging

conf_parser = configparser.ConfigParser()
conf_parser.read( "{}/settings.conf".format( os.path.dirname(__file__) ) )
conf = conf_parser["DEFAULT"]


# Database
DB_HOST = os.environ.get('DB_HOST', conf.get('DB_HOST', '127.0.0.1'))
DB_USER = conf["DB_USER"]
DB_PASSWORD = conf["DB_PASSWORD"]
DB_DATABASE = conf["DB_DATABASE"]
DB_POOL_SZ = conf.getint("DB_POOL_SZ", 4)

ENABLE_GZIP = conf.getboolean('ENABLE_GZIP', False)
GZIP_MINIMUM_SIZE = conf.getint('GZIP_MINIMUM_SIZE', 1000)

TEMP = conf.get('TEMP', '/opt/PANGmisc/DB_ROOT/TMP/')

LOG_LEVEL = conf.getint('LOG_LEVEL', logging.INFO)
