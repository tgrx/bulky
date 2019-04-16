import json
from decimal import Decimal
from typing import Dict, List, Optional, Text

import sqlalchemy as sa
from jinja2 import Template
from psycopg2.extensions import QuotedString, adapt
from sqlalchemy import Table
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Mapper, Session
from sqlalchemy.orm.attributes import InstrumentedAttribute
from typeguard import check_type, typechecked

from bulky import consts
from bulky import errors
from bulky.internals import sql
from bulky.types import (
    CleanReturningType,
    CleanedValuesSeriesType,
    ColumnPropertyType,
    ColumnType,
    ColumnTypesMapType,
    ReturningType,
    TableColumnsSetType,
    TableType,
    ValuesSeriesType,
    ValuesType,
)

_column_type_cache: Dict[Text, ColumnTypesMapType] = {}


@typechecked(always=True)
def get_table(table_or_model: TableType) -> Table:
    """
    Returns Table class for given Table Type

    :param table_or_model: SqlAlchemy table or mapper or model
    :return: SqlAlchemy Table class
    """

    if isinstance(table_or_model, Table):
        table = table_or_model
    else:
        table = table_or_model.__table__  # TODO: use sqlalchemy inspect

    return table


@typechecked(always=True)
def get_table_name(table_or_model: TableType) -> Text:
    """
    Returns table name in database for given Table or Model

    :param table_or_model: SqlAlchemy table or mapper or model
    :return: table name in database
    """

    inspected = sa.inspect(table_or_model)

    if isinstance(inspected, Table):
        table_name: Text = inspected.name
    elif isinstance(inspected, Mapper):
        table_name = inspected.tables[0].name
    else:  # pragma: no cover
        raise ValueError(f"unable to get table name for {table_or_model}")

    return table_name


@typechecked(always=True)
def is_db_type_comparable(db_type: Text) -> bool:
    """
    Checks if given database type is comparable in a trivial way.

    :param db_type: database name of column type
    :return: is type comparable via trivial equality test ("="), or not
    """

    if db_type.endswith("[]"):
        return False

    if db_type in consts.NON_COMPARABLE_DB_TYPES:
        return False

    return True


def clean_returning(
    table_or_model: TableType, returning: Optional[ReturningType]
) -> CleanReturningType:
    if not returning:
        return []

    columns = get_table_columns(table_or_model)
    cleaned = [
        sa.text(k)
        for k in sorted(
            {
                get_column_key(table_or_model, key, None, columns)
                for key_index, key in enumerate(returning)
            }
        )
    ]
    return cleaned


def validate_values(values: ValuesType, values_index: int):
    try:
        check_type("values", values, ValuesType)
    except TypeError:  # pragma: no cover
        raise errors.InvalidValueError(
            values_index, f"invalid type, expect: {ValuesType}"
        )

    if not values.keys():
        raise errors.InvalidValueError(values_index, "empty values")


@typechecked(always=True)
def clean_values(
    table_or_model: TableType,
    values_series: ValuesSeriesType,
    cast_db_types: bool = False,
    column_types: Optional[ColumnTypesMapType] = None,
) -> CleanedValuesSeriesType:
    """
    Cleans up and validates keys and values in values_series.

    :param table_or_model: SqlAlchemy table or mapper or model
    :param values_series: sequence of dicts with values
    :param cast_db_types: determines if need to cast values to db types
    :param column_types: column types map
    :return: sequence of cleaned values ({column name: value} dicts)
    """

    result: List = []

    if not values_series:
        return result

    # common columns used in values_list
    # expected to be the same in each values set
    columns_common = None

    columns_table = get_table_columns(table_or_model)

    # map: (column name | attr) -> column name
    columns_cleaned = {column: column for column in columns_table}

    column_types = column_types or {}

    # remap values: change dirty key to actual key for each values

    for values_index, values in enumerate(values_series):
        validate_values(values, values_index)

        columns_current = set()

        values_cleaned = {}

        # remap each key in values, keeping resulting key for alias

        for column_dirty, value in values.items():
            column_cleaned = columns_cleaned.get(column_dirty)

            if not column_cleaned:
                column_cleaned = get_column_key(
                    table_or_model, column_dirty, values_index, columns_table
                )
                columns_cleaned[column_cleaned] = column_cleaned

            if not cast_db_types:
                value_cleaned = value
            else:
                value_cleaned = to_db_literal(
                    value, cast_to=column_types.get(column_cleaned)
                )

            values_cleaned[column_cleaned] = value_cleaned

            columns_current.add(column_cleaned)

        # check that each values have the same set of keys

        columns_common = columns_common or frozenset(columns_current)

        columns_excess = columns_current - columns_common
        columns_missing = columns_common - columns_current

        if any((columns_excess, columns_missing)):
            raise errors.InvalidValueError(
                values_index,
                f"keys mismatch: excess={sorted(columns_excess)}, missing={sorted(columns_missing)}",
            )

        # add remapped values to result

        result.append(values_cleaned)

    return result


@typechecked(always=True)
def get_column_types(session: Session, table_or_model: TableType) -> ColumnTypesMapType:
    """
    Returns PostgreSQL types for columns of given table.

    :param session: SqlAlchemy session
    :param table_or_model: SqlAlchemy table or mapper or model
    :return: mapping between column name and db type name
    """

    table_name = get_table_name(table_or_model)

    if table_name in _column_type_cache:
        return _column_type_cache[table_name]

    stmt = Template(sql.STMT_GET_COLUMN_TYPES).render(table_name=table_name)

    response = session.execute(sa.text(stmt)).fetchall()

    result = {row.column_name: row.column_type for row in response}
    _column_type_cache[table_name] = result

    return result


def to_db_literal(value, cast_to=None):
    if value is None:
        return "null"
    elif isinstance(value, str):
        value = QuotedString(value.encode("utf-8")).getquoted().decode("utf-8")
        return value
    elif isinstance(value, bool):
        value = int(value)
        return value
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, (dict, list)):
        if cast_to == "json" or cast_to == "jsonb":
            value = json.dumps(value)
            return QuotedString(value).getquoted().decode("utf-8")
        elif cast_to == "hstore":
            assert isinstance(value, dict)  # TODO: discover if `assert` is enough
            s = ",".join(
                '"{}"=>{}'.format(k, "NULL" if v is None else '"{}"'.format(v))
                for k, v in value.items()
            )
            return QuotedString(s).getquoted().decode("utf-8")
        else:
            return adapt(value).getquoted().decode("utf-8")
    elif isinstance(value, Decimal):
        return str(value)
    else:
        return QuotedString(str(value)).getquoted().decode("utf-8")


@typechecked(always=True)
def get_table_columns(table_or_model: TableType) -> TableColumnsSetType:
    """
    Returns a set of all keys of model/table fields/columns

    :param table_or_model: SqlAlchemy table or mapper or model
    :return: set of column names (keys)
    """

    inspected = sa.inspect(table_or_model)
    columns = frozenset(inspected.columns.keys())

    return columns


@typechecked(always=True)
def get_column_key(
    table_or_model: TableType,
    column: ColumnType,
    value_index: Optional[int] = None,
    columns: Optional[TableColumnsSetType] = None,
) -> Text:
    """
    Resolves and returns a column key (name), with validations.

    :param table_or_model: SqlAlchemy table or mapper or model
    :param column: a column which expected to belong to table
    :param value_index: index of values in values_series for which column key is resolved
    :param columns: set of column names. Will be resolved if empty.
    :return: validated name of the column
    """

    columns = columns or get_table_columns(table_or_model)

    if isinstance(column, Text):
        if column not in columns:
            raise errors.InvalidColumnError(column, "not in table", value_index)

        # XXX: we need table attribute to check then if it is scalar

        if isinstance(table_or_model, DeclarativeMeta):
            column = getattr(table_or_model, column)
        else:
            column = table_or_model.columns[column]

    if isinstance(column, InstrumentedAttribute):
        check_type("column.property", column.property, ColumnPropertyType)

    return str(column.key)
