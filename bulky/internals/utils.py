import json
from decimal import Decimal
from typing import Dict, Sequence, Union, Any

import sqlalchemy as sa
from psycopg2._psycopg import QuotedString, adapt
from sqlalchemy import Table
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Mapper, Session
from sqlalchemy.orm.attributes import InstrumentedAttribute

from bulky import consts
from bulky import errors

_column_type_cache = {}


def is_type_comparable(type_name: str) -> bool:
    """
    Checks if given DB type is comparable in a trivial way
    """

    if not isinstance(type_name, str):
        return False

    if type_name.endswith("[]"):
        return False

    if type_name in consts.NON_COMPARABLE_TYPES:
        return False

    return True


def clean_values(
    table: Union[Table, Mapper],
    values_list: Sequence[dict],
    cast_db_types: bool = False,
    column_types: Dict[str, str] = None,
) -> Sequence[Dict[str, Any]]:
    """
    Cleans up and validates keys and values in values_list

    :param table: SqlAlchemy table or mapper
    :param values_list: sequence of dicts with values
    :param cast_db_types: determines if need to cast values to db types
    :param column_types: column types map
    :return: sequence of cleaned values (column: value dicts)
    """

    if not isinstance(values_list, consts.ALLOWED_VALUES_TYPES):
        raise errors.BulkOperationError(
            "Invalid values type {vt}, expected: {allowed}".format(
                vt=type(values_list), allowed=consts.ALLOWED_VALUES_TYPES
            )
        )

    result = []

    keys_general = None  # general, common keys used in values_list
    columns = get_table_columns(table)

    # map: (column name | attr) -> column name
    aliased_keys = {column: column for column in columns}

    # remap values: change key alias to actual key for each values

    column_types = column_types or {}

    for idx, values in enumerate(values_list):
        if not isinstance(values, dict):
            raise errors.InvalidValueError(idx, "not a dict")

        if not tuple(values.keys()):  # FIXME: iterate / any!!!
            raise errors.InvalidValueError(idx, "empty values")

        keys_current = set()

        cleaned_values = {}

        # remap each key in values, keeping resulting key for alias

        for key_alias, value in values.items():
            key = aliased_keys.get(key_alias)

            if not key:
                key = get_column_key(table, key_alias, idx, columns)
                aliased_keys[key_alias] = key

            if not cast_db_types:
                cleaned_values[key] = value
            else:
                cleaned_values[key] = to_db_literal(
                    value, cast_to=column_types.get(key)
                )

            keys_current.add(key)

        # check that each values have the same set of keys

        keys_general = keys_general or frozenset(keys_current)

        keys_added = keys_current - keys_general
        keys_missed = keys_general - keys_current

        if any((keys_added, keys_missed)):
            msg = "keys mismatch: added={added}, missed={missed}".format(
                added=sorted(keys_added), missed=sorted(keys_missed)
            )
            raise errors.InvalidValueError(idx, msg)

        # add remapped values to result

        result.append(cleaned_values)

    return result


def get_column_types(session: Session, table: Union[Table, Mapper]) -> Dict[str, str]:
    """
    Returns PostgreSQL column types for columns of given table
    """

    inspected = sa.inspect(table)
    if isinstance(inspected, Table):
        tablename = inspected.name
    elif isinstance(inspected, Mapper):
        tablename = inspected.tables[0].name
    else:
        raise TypeError("Invalid table {}".format(table))

    if tablename in _column_type_cache:
        return _column_type_cache[tablename]

    stmt = """
    SELECT
    c.column_name,
    (
        CASE c.data_type
        WHEN 'ARRAY'
            THEN e.data_type || '[]'
        WHEN 'USER-DEFINED'
            THEN c.udt_name
        ELSE c.data_type
        END
    ) AS column_type
    FROM information_schema.columns c
        LEFT JOIN information_schema.element_types e
            ON (
                (
                    c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    'TABLE',
                    c.dtd_identifier
                ) = (
                    e.object_catalog,
                    e.object_schema,
                    e.object_name,
                    e.object_type,
                    e.collection_type_identifier
                )
            )
    WHERE c.table_schema = 'public' AND c.table_name = '{table}'
    ORDER BY c.ordinal_position
    ;
    """.format(
        table=tablename
    )

    response = session.execute(stmt).fetchall()

    result = {row.column_name: row.column_type for row in response}
    _column_type_cache[tablename] = result
    return result


def to_db_literal(value, cast_to=None):
    if value is None:
        return "null"
    elif isinstance(value, str):
        value = QuotedString(value.encode("utf-8")).getquoted().decode("utf-8")
        return value
    elif isinstance(value, (int, float)):
        return value
    elif isinstance(value, bool):
        value = QuotedString(str(value).lower()).getquoted().decode("utf-8")
        return value
    elif isinstance(value, (dict, list)):
        if cast_to == "json" or cast_to == "jsonb":
            value = json.dumps(value)
            return QuotedString(value).getquoted().decode("utf-8")
        elif cast_to == "hstore":
            assert isinstance(value, dict)
            s = ",".join(
                '"{}"=>{}'.format(k, "NULL" if not v else '"{}"'.format(v))
                for k, v in value.items()
            )
            return QuotedString(s).getquoted().decode("utf-8")
        else:
            return adapt(value).getquoted().decode("utf-8")
    elif isinstance(value, Decimal):
        return str(value)
    else:
        return QuotedString(str(value)).getquoted().decode("utf-8")


def get_table_columns(table: Union[Table, Mapper]) -> frozenset:
    """
    Returns a set of all keys of model/table fields/columns
    """

    if not isinstance(table, consts.ALLOWED_TABLE_TYPES):
        raise TypeError("Invalid table {}".format(table))

    inspected = sa.inspect(table)
    columns = frozenset(inspected.columns.keys())

    return columns


def get_column_key(
    table: Union[Table, Mapper],
    column: consts.ALLOWED_COLUMN_TYPES,
    value_index: int = None,
    columns: frozenset = None,
) -> str:
    columns = columns or get_table_columns(table)

    if not isinstance(column, consts.ALLOWED_COLUMN_TYPES):
        raise errors.InvalidColumnError(column, "Unsupported column type", value_index)

    if isinstance(column, str):
        if column not in columns:
            raise errors.InvalidColumnError(column, "Not in model", value_index)

        # XXX: we need table attribute to check then if it is scalar

        if isinstance(table, DeclarativeMeta):
            column = getattr(table, column)
        else:
            column = table.columns[column]

    if isinstance(column, InstrumentedAttribute):
        if not isinstance(column.property, consts.ALLOWED_COLUMN_PROPS):
            raise errors.InvalidColumnError(column, "Not a column attribute")

    return column.key
