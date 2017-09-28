"""
This procedure will rebuild the sqlite lkb database
"""

import logging
from lib import my_env
from lib import sqlite_store

cfg = my_env.init_env("catw_migrate", __file__)
logging.info("Start application")
catw = sqlite_store.DirectConn(cfg)
catw.rebuild()
logging.info("sqlite catw rebuild")
logging.info("End application")
