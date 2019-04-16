import sqlalchemy as sa

from bulky import consts, errors, insert
from tests.db import *


class InsertTest(BulkyTest):
    def test_single(self):
        dataset = [{Model.v_text: 123, Model.v_int: 321}]
        returning = (Model.id, Model.v_text, Model.v_int)

        rows = insert(self.session, Model, dataset, returning)
        self.assertEqual(1, len(rows), "wrong amount of rows returned")

        row = rows[0]
        self.assertTrue(row, "invalid row data returned")
        self.assertEqual(3, len(row), "invalid row data returned")
        self.assertTrue(
            hasattr(row, Model.id.key), f"row has no `{Model.id.key}` column"
        )
        self.assertTrue(
            hasattr(row, Model.v_text.key), f"row has no `{Model.v_text.key}` column"
        )
        self.assertTrue(
            hasattr(row, Model.v_int.key), f"row has no `{Model.v_int.key}` column"
        )
        self.assertTrue(
            row.id, f"wrong value of primary key in `{Model.id.key}` column"
        )
        self.assertEqual(
            row.v_text, "123", f"wrong value in `{Model.v_text.key}` column"
        )
        self.assertEqual(row.v_int, 321, f"wrong value in `{Model.v_int.key}` column")

    def test_empty_dataset(self):
        rows = insert(self.session, Model, [])
        self.assertFalse(rows, "unexpected rows on empty dataset")

    def test_no_returning(self):
        dataset = [{Model.v_int: 1}]

        rows = insert(self.session, Model, dataset)
        self.assertFalse(rows, "unexpected rows when no returning requested")

        query = sa.select([Model.id, Model.v_int])

        rows = self.session.execute(query).fetchall()
        self.assertTrue(rows, "no data were inserted")
        self.assertEqual(
            1, len(rows), "wrong amount data are in target table after insert"
        )

        row = rows[0]
        self.assertTrue(
            row.id, f"wrong value of primary key in `{Model.id.key}` column"
        )
        self.assertEqual(row.v_int, 1, f"wrong value in `{Model.v_int.key}` column")

    def test_bulk(self):
        values_expected = {(i, str(i)) for i in range(consts.BULK_CHUNK_SIZE + 10)}

        rows = insert(
            self.session,
            Model,
            [{Model.v_int: i, Model.v_text: j} for i, j in values_expected],
            [Model.v_int, Model.v_text],
        )
        self.assertTrue(rows, "insert does not return data")
        self.assertEqual(
            len(values_expected),
            len(rows),
            "wrong amount of data are in table after insert",
        )

        values_returned = {(row.v_int, row.v_text) for row in rows}
        self.assertSetEqual(
            values_expected, values_returned, "wrong data are in table after insert"
        )

    def test_errors_wrong_data_column(self):
        dataset = [{Model.v_int: 1, "unknown_column": 2}]

        with self.assertRaises(errors.InvalidColumnError) as arc:
            insert(self.session, Model, dataset)
        self.assertEqual(
            "invalid key `unknown_column` in values_series[0]: not in table",
            str(arc.exception),
        )

    def test_errors_wrong_returning_column(self):
        dataset = [{Model.v_int: 1}]

        with self.assertRaises(errors.InvalidColumnError) as arc:
            insert(self.session, Model, dataset, ["unknown_column"])
        self.assertEqual(
            "invalid key `unknown_column` in values_series: not in table",
            str(arc.exception),
        )

    def test_sqlalchemy_default_value(self):
        dataset = [{Model.v_int: 1}]
        returning = (Model.id, Model.v_int, Model.v_default)

        rows = insert(self.session, Model, dataset, returning)
        self.assertEqual(1, len(rows), "wrong amount of rows returned")

        row = rows[0]
        self.assertTrue(row, "invalid row data returned")
        self.assertEqual(3, len(row), "invalid row data returned")
        self.assertTrue(
            hasattr(row, Model.id.key), f"row has no `{Model.id.key}` column"
        )
        self.assertTrue(
            hasattr(row, Model.v_int.key), f"row has no `{Model.v_int.key}` column"
        )
        self.assertTrue(
            hasattr(row, Model.v_default.key),
            f"row has no `{Model.v_default.key}` column",
        )
        self.assertTrue(
            row.id, f"wrong value of primary key in `{Model.id.key}` column"
        )
        self.assertEqual(row.v_int, 1, f"wrong value in `{Model.v_int.key}` column")
        self.assertEqual(
            row.v_default, 31337, f"wrong value in `{Model.v_default.key}` column"
        )
