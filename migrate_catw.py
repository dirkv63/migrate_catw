"""
This is the script to migrate the catw database from mysql to sqlite.
"""

from lib import my_env
from lib import sqlite_store
from lib.sqlite_store import *

cfg = my_env.init_env("catw_migrate", __file__)
db = cfg['Main']['db']
logging.info("Start application")
# cons_sess = mysql.init_session()
catw = sqlite_store.init_session(db=db)
cons_sess = sqlite_store.init_mysql(
    db=cfg['catw']['db'],
    user=cfg['catw']['user'],
    pwd=cfg['catw']['passwd']
)
tables = [Parameter, Project, Timesheet]
for obj in tables:
    query = cons_sess.query(obj)
    for rec in query:
        attribs = {}
        for col in obj.__table__.columns.keys():
            attribs[col] = getattr(rec, col)
        new_rec = obj(**attribs)
        catw.add(new_rec)
    catw.commit()
