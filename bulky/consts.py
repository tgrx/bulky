from sqlalchemy import Column, Table
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import ColumnProperty
from sqlalchemy.orm.attributes import InstrumentedAttribute

ALLOWED_COLUMN_PROPS = (ColumnProperty,)
ALLOWED_COLUMN_TYPES = (str, InstrumentedAttribute, Column)
ALLOWED_TABLE_TYPES = (DeclarativeMeta, Table)
ALLOWED_VALUES_TYPES = (list, tuple)
BULK_CHUNK_SIZE = 10000

NON_COMPARABLE_TYPES = frozenset(("json",))
