__all__ = ("configure", "insert", "update")

from sqlalchemy.ext.declarative import declarative_base

from bulky.impl.insert import insert
from bulky.impl.update import update


def configure(base: declarative_base = None):
    from bulky import conf

    conf.Base = base
