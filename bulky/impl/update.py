from typing import Sequence, Union

from jinja2 import Template
from multiprocessing import Queue, cpu_count
from sqlalchemy import Table
from sqlalchemy.orm import Mapper, Session

from bulky import consts
from bulky.internals import sql
from bulky.internals import utils
from bulky.internals.bundles import UpdateRenderBundle
from bulky.internals.rendering import UpdateQueryRenderer

_template = Template(sql.STMT_UPDATE)


def update(
    session: Session,
    table: Union[Table, Mapper],
    values_list: Sequence[dict],
    returning: Sequence = None,
    reference_field: Union[str, list] = "id",
) -> Union[list, None]:
    """
    Performs a bulk update query issued bypassing session cache
    :param session: SQLAlchemy session
    :param table: a table to insert data
    :param values_list: list of labelled values (list of dicts)
    :param returning: specifies which fields to return right after inserting
    :param reference_field: fields to identify rows
    :return: list of returning values or None
    """

    if not values_list:
        return None

    if isinstance(table, Table):
        table = table
    else:
        table = table.__table__

    column_types = utils.get_column_types(session, table)
    values_list = utils.clean_values(
        table, values_list, cast_db_types=True, column_types=column_types
    )

    columns = frozenset(values_list[0].keys())

    if isinstance(reference_field, (list, tuple)):
        reference_fields = frozenset(
            utils.get_column_key(table, f) for f in reference_field
        )
    else:
        reference_fields = frozenset([utils.get_column_key(table, reference_field)])

    if reference_fields - columns:
        raise ValueError(
            "reference field {rf} does not exist in table {tbl}".format(
                rf=sorted(reference_fields), tbl=table.name
            )
        )

    columns_to_update = sorted(columns - reference_fields)
    columns = sorted(columns)
    reference_fields = sorted(reference_fields)

    update_changed = all(
        utils.is_type_comparable(column_types[column]) for column in columns_to_update
    )

    returning = list(
        utils.get_column_key(table, column) for column in (returning or [])
    )

    chunked_values = (
        values_list[i : i + consts.BULK_CHUNK_SIZE]
        for i in range(0, len(values_list), consts.BULK_CHUNK_SIZE)
    )

    conn = session.connection().execution_options(no_parameters=True)

    result = []

    for chunk in chunked_values:
        stmt = _template.render(
            src="src",
            dst=table.name,
            columns=columns,
            values_list=chunk,
            column_types=column_types,
            columns_to_update=columns_to_update,
            update_changed=update_changed,
            reference_fields=reference_fields,
            returning=returning,
        )

        response = conn.execute(stmt)

        if returning:
            result.extend(response.fetchall())

    return result


def _render_queries(
    table,
    values_list,
    returning,
    columns,
    column_types,
    columns_to_update,
    reference_fields,
):
    values_chunks = (
        values_list[i : i + consts.BULK_CHUNK_SIZE]
        for i in range(0, len(values_list), consts.BULK_CHUNK_SIZE)
    )

    # render queries

    bundles = [
        UpdateRenderBundle(
            table=table.name,
            values_list=chunk,
            returning=returning,
            columns=columns,
            column_types=column_types,
            columns_to_update=columns_to_update,
            reference_fields=reference_fields,
        )
        for chunk in values_chunks
    ]

    # queues for bundles with params for rendering and for result queries

    bundles_q = Queue()
    queries_q = Queue()

    # start worker processes

    workers = [UpdateQueryRenderer(bundles_q, queries_q) for _ in range(cpu_count())]

    for w in workers:
        w.start()

    # populate bundles queue with bundles and poison pills

    for bundle in bundles:
        bundles_q.put(bundle)

    for _ in range(cpu_count()):
        bundles_q.put(None)

    # collect rendered queries from workers

    result = []

    for _ in bundles:
        q = queries_q.get(timeout=30)
        result.append(q)

    # close workers

    for w in workers:
        w.join()

    return result
