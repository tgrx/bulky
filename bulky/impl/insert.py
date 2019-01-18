from multiprocessing import Queue, cpu_count
from threading import Thread
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy import Table
from sqlalchemy.orm import Session

from bulky import conf
from bulky import consts
from bulky.internals import bundles
from bulky.internals import utils
from bulky.internals.rendering import InsertQueryRenderer


def _render_queries(table, values_list, returning, columns, column_types):
    values_chunks = (
        values_list[i : i + consts.BULK_CHUNK_SIZE]
        for i in range(0, len(values_list), consts.BULK_CHUNK_SIZE)
    )

    # render queries

    bundles_list = [
        bundles.InsertRenderBundle(
            table=table.name,
            values_list=chunk,
            returning=returning,
            columns=columns,
            column_types=column_types,
        )
        for chunk in values_chunks
    ]

    # queues for bundles with params for rendering and for result queries

    bundles_q = Queue()
    queries_q = Queue()

    # start worker processes

    workers = [InsertQueryRenderer(bundles_q, queries_q) for _ in range(cpu_count())]

    for w in workers:
        w.start()

    # populate bundles queue with bundles and poison pills

    for bundle in bundles_list:
        bundles_q.put(bundle)

    for _ in range(cpu_count()):
        bundles_q.put(None)

    # collect rendered queries from workers

    result = []

    for _ in bundles_list:
        q = queries_q.get(timeout=30)
        if q:
            result.append(q)

    # close workers

    for w in workers:
        w.join()

    return result


def _execute_query(bundle):
    """
    Issues an INSERT query into DB collecting the returning result
    """

    conn = bundle.session.connection().execution_options(no_parameters=True)

    for q in bundle.queries:
        response = conn.execute(q)

        if bundle.returning:
            data = response.fetchall()
            bundle.result.extend(data)


def _do_insert(session: Session, queries: list, returning: list, result: list):
    """
    Issues INSERT raw queries into DB simultaneously
    """

    n_threads = cpu_count()

    chunk_size = len(queries) // n_threads
    chunk_size = chunk_size or 1

    queries_chunks = (
        queries[i : i + chunk_size] for i in range(0, len(queries), chunk_size)
    )

    workers = []

    for chunk in queries_chunks:
        bundle = bundles.InsertExecuteBundle(
            session=session, returning=returning, result=result, queries=chunk
        )

        worker = Thread(target=_execute_query, args=[bundle])

        workers.append(worker)
        worker.start()

    for worker in workers:
        worker.join()


def insert(
    session: Session,
    table: Union[Table, conf.Base],
    values_list: Sequence[dict],
    returning: Sequence = None,
) -> list:
    """
    Inserts a bulk of values into DB using SqlAlchemy session
    Uses no multiprocessing & multi-threading
    """

    result = []

    if not values_list:
        return result

    values_list = utils.clean_values(table, values_list)

    if isinstance(table, Table):
        table = table
    else:
        table = table.__table__

    chunked_values = (
        values_list[i : i + consts.BULK_CHUNK_SIZE]
        for i in range(0, len(values_list), consts.BULK_CHUNK_SIZE)
    )

    for n_chunk, chunk in enumerate(chunked_values):
        query = sa.insert(table, values=chunk, returning=returning, inline=True)
        query_result = session.execute(query)

        if returning:
            data = query_result.fetchall()
            result.extend(data)

    if returning:
        return result
