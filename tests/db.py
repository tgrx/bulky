import unittest
from contextlib import closing

import sqlalchemy as sa
from dynaconf import settings
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

try:
    DATABASE_URL = settings.DATABASE_URL
    assert DATABASE_URL, "database is not configured"
except AttributeError as err:
    raise AssertionError("database is not configured")

Base = declarative_base()


class Model(Base):  # type: ignore
    __tablename__ = "t"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    v_default = sa.Column(sa.Integer, default="31337")

    v_array = sa.Column(ARRAY(sa.Text))
    v_bool = sa.Column(sa.Boolean)
    v_date = sa.Column(sa.Date)
    v_datetime = sa.Column(sa.DateTime)
    v_float = sa.Column(sa.Float)
    v_int = sa.Column(sa.Integer)
    v_numeric = sa.Column(sa.Numeric)
    v_text = sa.Column(sa.Text)


def get_engine():
    engine = create_engine(DATABASE_URL)
    return engine


def create_tables(engine):
    Model.metadata.create_all(engine)


def drop_tables(engine):
    with closing(engine.connect()) as conn:
        conn = conn.execution_options(autocommit=True)

        conn.execute(sa.text("DROP TABLE IF EXISTS {}".format(Model.__tablename__)))


def db_setup():
    engine = get_engine()
    drop_tables(engine)
    create_tables(engine)


def db_teardown():
    engine = get_engine()
    drop_tables(engine)


class BulkyTest(unittest.TestCase):
    longMessage = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        db_setup()

    @classmethod
    def tearDownClass(cls):
        db_teardown()
        super().tearDownClass()

    def setUp(self):
        super().setUp()

        self._engine = get_engine()
        self._connection = self._engine.connect()

        try:
            self._transaction = self._connection.begin()
        except:
            self._connection.close()

        try:
            self.session = Session(bind=self._connection)
        except:
            try:
                self._transaction.rollback()
            finally:
                self._connection.close()

    def tearDown(self):
        try:
            self._transaction.rollback()
        finally:
            self._connection.close()

        self._engine = self._connection = self._transaction = None

        super().tearDown()


__all__ = ("BulkyTest", "Model")
