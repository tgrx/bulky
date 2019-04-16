from typing import Optional

import sqlalchemy as sa
from typeguard import typechecked

from bulky import consts
from bulky.internals import utils
from bulky.types import (
    ReturningType,
    RowsType,
    SessionType,
    TableType,
    ValuesSeriesType,
)


@typechecked(always=True)
def insert(
    session: SessionType,
    table_or_model: TableType,
    values_series: ValuesSeriesType,
    returning: Optional[ReturningType] = None,
) -> RowsType:
    """
    Inserts a series of values into DB.

    Data are split into chunks.
    Chunks are inserted sequentially.

    No multiprocessing.
    No multithreading.
    No async IO.

    The order of data elements is not preserved.
    The order of returned rows is undefined.

    Session is not flushed.
    Inserted objects are not propagated to session.
    Values are not validated against database type.
    Default values on SqlAlchemy level are resolved and populated implicitly.

    :param session: session from SqlAlchemy

    :param table_or_model: a Table or Mapper or class inherited from declarative_base() call

    :param values_series: a sequence of values in {column: value} format.
        `column` may be:
            * a name of a table column;
            * a column attribute of a table / Mapper / Declarative;

    :param returning: a sequence of elements representing table / Mapper / Declarative columns.
        These columns, bound with values, will be returned after insert.

    :return: a list of RowProxy.
        If either no data are inserted or no returning requested, empty list will be returned.
    """

    result: RowsType = []

    if not values_series:
        return result

    table = utils.get_table(table_or_model)

    returning_cleaned = utils.clean_returning(table, returning)
    values_series_cleaned = utils.clean_values(table, values_series)

    values_series_chunks = (
        values_series_cleaned[i : i + consts.BULK_CHUNK_SIZE]
        for i in range(0, len(values_series_cleaned), consts.BULK_CHUNK_SIZE)
    )

    for n_chunk, chunk in enumerate(values_series_chunks):
        query = sa.insert(table, values=chunk, returning=returning_cleaned, inline=True)
        query_result = session.execute(query)

        if returning:
            data = query_result.fetchall()
            result.extend(data)

    return result
