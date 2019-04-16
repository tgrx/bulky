from typing import Any, Dict, FrozenSet, Iterable, List, Sequence, Text, Union

from sqlalchemy import Column, Table, text
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Mapper, Session, ColumnProperty
from sqlalchemy.orm.attributes import InstrumentedAttribute

TableType = Union[Table, Mapper, DeclarativeMeta]
TableColumnsSetType = FrozenSet[Text]
ColumnType = Union[Text, InstrumentedAttribute, Column]
ColumnPropertyType = ColumnProperty
ColumnTypesMapType = Dict[Text, Text]
SessionType = Session

ValuesType = Dict[ColumnType, Any]
ValuesSeriesType = Sequence[ValuesType]
CleanedValuesType = Dict[Text, Any]
CleanedValuesSeriesType = Sequence[CleanedValuesType]

ReturningType = Sequence[ColumnType]
CleanReturningType = Sequence[text]

ReferenceType = Iterable[ColumnType]

RowType = Any  # TODO: ResultProxy failed: mypy="invalid type" why ???
RowsType = List[RowType]
