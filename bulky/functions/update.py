from typing import List, Optional

from jinja2 import Template

from bulky import consts
from bulky.internals import sql
from bulky.internals import utils
from bulky.types import (
    ReferenceType,
    ReturningType,
    RowsType,
    SessionType,
    TableType,
    ValuesSeriesType,
)

_template = Template(sql.STMT_UPDATE)


def update(
    session: SessionType,
    table_or_model: TableType,
    values_series: ValuesSeriesType,
    returning: Optional[ReturningType] = None,
    reference: ReferenceType = ("id",),
) -> RowsType:
    """
    Performs a bulk update query issued bypassing session cache
    :param session: SQLAlchemy session
    :param table_or_model: a table to insert data
    :param values_series: list of labelled values (list of dicts)
    :param returning: specifies which fields to return right after inserting
    :param reference: fields to identify rows
    :return: list of returning values or None
    """

    if not values_series:
        return []

    table = utils.get_table(table_or_model)

    column_types = utils.get_column_types(session, table)
    values_series = utils.clean_values(
        table, values_series, cast_db_types=True, column_types=column_types
    )

    columns = frozenset(values_series[0].keys())

    reference_fields = frozenset(utils.get_column_key(table, f) for f in reference)

    if reference_fields - columns:
        raise ValueError(
            "reference field {rf} does not exist in table {tbl}".format(
                rf=sorted(reference_fields), tbl=table.name
            )
        )

    columns_to_update = sorted(columns - reference_fields)
    columns_sorted = sorted(columns)
    reference_fields_sorted = sorted(reference_fields)

    update_changed = all(
        utils.is_db_type_comparable(column_types[column])
        for column in columns_to_update
    )

    returning = list(
        utils.get_column_key(table, column) for column in (returning or [])
    )

    chunked_values = (
        values_series[i : i + consts.BULK_CHUNK_SIZE]
        for i in range(0, len(values_series), consts.BULK_CHUNK_SIZE)
    )

    conn = session.connection().execution_options(no_parameters=True)

    result: List = []

    for chunk in chunked_values:
        stmt = _template.render(
            src="src",
            dst=table.name,
            columns=columns_sorted,
            values_list=chunk,
            column_types=column_types,
            columns_to_update=columns_to_update,
            update_changed=update_changed,
            reference_fields=reference_fields_sorted,
            returning=returning,
        )

        response = conn.execute(stmt)

        if returning:
            result.extend(response.fetchall())

    return result
