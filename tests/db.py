import os
import unittest
from contextlib import closing

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

DATABASE_URL = os.getenv("DATABASE_URL")

Base = declarative_base()


class Model(Base):
    __tablename__ = "t"

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    v_array = sa.Column(ARRAY(sa.Text))
    v_bool = sa.Column(sa.Boolean)
    v_date = sa.Column(sa.Date)
    v_datetime = sa.Column(sa.DateTime)
    v_float = sa.Column(sa.Float)
    v_int = sa.Column(sa.Integer)
    v_numeric = sa.Column(sa.Numeric)
    v_text = sa.Column(sa.Text)


def get_engine():
    if not DATABASE_URL:
        raise RuntimeError("DB is not configured")

    engine = create_engine(DATABASE_URL, isolation_level="AUTOCOMMIT")
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

        super().tearDown()


__all__ = ("BulkyTest", "Model")
