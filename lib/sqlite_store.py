"""
This module consolidates Database access for the lkb project.
"""

import datetime
import logging
import os
import sqlite3
from sqlalchemy import Column, Integer, Text, create_engine, String, Date, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(10), index=True, unique=True)
    password_hash = Column(String(256))

    def __repr__(self):
        return "<User: {user}>".format(user=self.username)


class Project(Base):
    __tablename__ = "projects"
    project_id = Column(Integer, primary_key=True)
    wbs = Column(String(256))
    name = Column(String(256))
    start = Column(Date)
    end = Column(Date)
    entered = Column(DateTime, default=datetime.datetime.utcnow)
    status = Column(String(256))
    billable = Column(String(256))
    info = Column(Text)
    cats = relationship('Timesheet', back_populates='project')

    def __repr__(self):
        return "<Project: {name}>".format(name=self.name)


class Timesheet(Base):
    __tablename__ = "timesheet"
    project_id = Column(Integer, ForeignKey('projects.project_id'), primary_key=True)
    datestring = Column(Date(), primary_key=True)
    timestring = Column(Integer)
    project = relationship('Project', back_populates='cats')

    def __repr__(self):
        return "<Time Entry: Project ID {p} - Date {d} - Worked {w}>".format(p=self.project_id,
                                                                             d=self.datestring,
                                                                             w=self.timestring)


class Parameter(Base):
    __tablename__ = 'parameters'
    parameter = Column(String(255), nullable=False, primary_key=True)
    value = Column(String(255), nullable=False)

    def __repr__(self):
        return "<{key}: {value}>".format(key=self.parameter, value=self.value)


class DirectConn:
    """
    This class will set up a direct connection to the database. It allows to reset the database,
    in which case the database will be dropped and recreated, including all tables.
    """

    def __init__(self, config):
        """
        To drop a database in sqlite3, you need to delete the file.
        """
        self.db = config['Main']['db']
        self.dbConn = ""
        self.cur = ""

    def _connect2db(self):
        """
        Internal method to create a database connection and a cursor. This method is called during object
        initialization.
        Note that sqlite connection object does not test the Database connection. If database does not exist, this
        method will not fail. This is expected behaviour, since it will be called to create databases as well.

        :return: Database handle and cursor for the database.
        """
        logging.debug("Creating Datastore object and cursor")
        db_conn = sqlite3.connect(self.db)
        # db_conn.row_factory = sqlite3.Row
        logging.debug("Datastore object and cursor are created")
        return db_conn, db_conn.cursor()

    def rebuild(self):
        # A drop for sqlite is a remove of the file
        try:
            os.remove(self.db)
        except FileNotFoundError:
            # If the file is not there, then do not delete it.
            pass
        # Reconnect to the Database
        self.dbConn, self.cur = self._connect2db()
        # Use SQLAlchemy connection to build the database
        conn_string = "sqlite:///{db}".format(db=self.db)
        engine = set_engine(conn_string=conn_string)
        Base.metadata.create_all(engine)


def init_mysql(db, user, pwd, echo=False):
    """
    This function configures the connection to the database and returns the session object.

    :param db: Name of the MySQL database.

    :param user: User for the connection.

    :param pwd: Connection password associated with this user.

    :param echo: True / False, depending if echo is required. Default: False

    :return: session object.
    """
    # conn_string = "mysql+pymysql://{u}:{p}@localhost/{db}?charset=utf8&use_unicode=0".format(db=db, u=user, p=pwd)
    conn_string = "mysql+pymysql://{u}:{p}@localhost/{db}?charset=utf8".format(db=db, u=user, p=pwd)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def init_session(db, echo=False):
    """
    This function configures the connection to the database and returns the session object.

    :param db: Name of the sqlite3 database.

    :param echo: True / False, depending if echo is required. Default: False

    :return: session object.
    """
    conn_string = "sqlite:///{db}".format(db=db)
    engine = set_engine(conn_string, echo)
    session = set_session4engine(engine)
    return session


def set_engine(conn_string, echo=False):
    engine = create_engine(conn_string, echo=echo)
    return engine


def set_session4engine(engine):
    session_class = sessionmaker(bind=engine)
    session = session_class()
    return session
