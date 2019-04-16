# bulky

A library which provides bulk insert and update operations for SQLAlchemy and PostgreSQL.


## Shields

![](https://img.shields.io/pypi/l/bulky.svg)  
![](https://img.shields.io/pypi/pyversions/bulky.svg)  
![](https://img.shields.io/pypi/status/bulky.svg)  
![](https://img.shields.io/pypi/wheel/bulky.svg)  
![](https://img.shields.io/pypi/implementation/bulky.svg)  
![](https://img.shields.io/pypi/dm/bulky.svg)  
![](https://img.shields.io/gitlab/pipeline/tgrx/bulky/master.svg)  
[![coverage report](https://gitlab.com/tgrx/bulky/badges/master/coverage.svg)](https://gitlab.com/tgrx/bulky/commits/master)  
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

## Example of usage

### insert

Using of `bulky.insert` is quite simple.
Just pass the series of values in `{column: value}` form and that's it.
This function uses no multiprocessing, no multithreading, no asyncio.

You can return newly created rows as well.

```python
import bulky
from your.sqlalchemy.models import Model
from your.sqlalchemy.session import Session

data = [
    {Model.column_float: random()}
    for _ in range(100_000_000)
]

rows_inserted = bulky.insert(
    session=Session,
    table_or_model=Model,
    values_series=data,
    returning=[Model.id, Model.column_float]
)

new_items = {row.id: row.column_float for row in rows_inserted}
```

### update

Using of `bulky.update` is quite simple as well, however there are some notes, see below.

This function uses no multiprocessing, no multithreading, no asyncio.

The first use case is simple.
You pass the series of values and specify the returning columns.
However, values must have a value for reference column (usually a primary key, named "id" - this is default).

```python
import bulky
from your.sqlalchemy.models import Model
from your.sqlalchemy.session import Session

data = [
    {
        Model.id: i,
        Model.column_integer: i * 100,
    }
    for i in range(100_000_000)
]

rows_updated = bulky.update(
    session=Session,
    table_or_model=Model,
    values_series=data,
    returning=[Model.id, Model.column_integer],
)

updated_items = {row.id: row.column_integer for row in rows_updated}
```

You can use a complex reference (when your primary key is consisted of two or more columns):

```python
import bulky
from your.sqlalchemy.models import ManyToManyTable
from your.sqlalchemy.session import Session

data = [
    {
        ManyToManyTable.fk1: i,
        ManyToManyTable.fk2: j,
        ManyToManyTable.value: i + j,
    }
    for i in range(100_000_000)
    for j in range(100_000_000)
]

rows_updated = bulky.update(
    session=Session,
    table_or_model=ManyToManyTable,
    values_series=data,
    returning=[
        ManyToManyTable.fk1,
        ManyToManyTable.fk2,
        ManyToManyTable.value,],
    reference=[
        ManyToManyTable.fk1,
        ManyToManyTable.fk2,],
)

updated_items = {(row.fk1, row.fk2): row.value for row in rows_updated}
```

The `update` function actually updates only those rows which values differ from stored:

```python
import bulky
from your.sqlalchemy.models import Model
from your.sqlalchemy.session import Session

data = [
    {
        Model.id: i,
        Model.column_integer: i * 100,
    }
    for i in range(100_000_000)
]

rows_updated = bulky.update(
    session=Session,
    table_or_model=Model,
    values_series=data,
    returning=[Model.id],
)

assert len(rows_updated) == 100_000_000

rows_updated = bulky.update(
    session=Session,
    table_or_model=Model,
    values_series=data,
    returning=[Model.id],
)

assert len(rows_updated) == 0
```
