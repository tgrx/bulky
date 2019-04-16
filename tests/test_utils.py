from decimal import Decimal

from sqlalchemy import Table

from bulky import errors
from bulky.internals import utils as u
from tests.db import *


class UtilsTest(BulkyTest):
    def test_get_table(self):
        tbl = u.get_table(Model)
        self.assertIsInstance(
            tbl, Table, f"invalid table type: expected {Table}, got {type(tbl)}"
        )
        self.assertEqual(
            "t", tbl.name, f"invalid table returned: expected 't', got '{tbl.name}'"
        )

        tbl = u.get_table(tbl)
        self.assertIsInstance(
            tbl, Table, f"invalid table type: expected {Table}, got {type(tbl)}"
        )
        self.assertEqual(
            "t", tbl.name, f"invalid table returned: expected 't', got '{tbl.name}'"
        )

    def test_get_table_name(self):
        name = u.get_table_name(Model)
        self.assertEqual(
            "t", name, f"invalid table name returned: expected 't', got '{name}'"
        )

    def test_is_db_type_comparable(self):
        db_types = {"int": True, "float": True, "int[]": False, "json": False}

        for db_type, expected in db_types.items():
            got = u.is_db_type_comparable(db_type)
            self.assertEqual(
                expected,
                got,
                f"wrong comparable status for type `{db_type}: expected {expected}, got {got}",
            )

    def test_clean_returning(self):
        self.assertEqual([], u.clean_returning(Model, []))

        r_attrs = [Model.id, Model.v_default, Model.v_int]
        r_keys = [_a.key for _a in r_attrs]

        self.assertSequenceEqual(
            r_keys, [_c.text for _c in u.clean_returning(Model, r_attrs)]
        )
        self.assertSequenceEqual(
            r_keys, [_c.text for _c in u.clean_returning(Model, r_keys)]
        )

        with self.assertRaises(errors.InvalidColumnError) as arc:
            u.clean_returning(Model, ["unknown_column"])
        self.assertEqual(
            str(arc.exception),
            "invalid key `unknown_column` in values_series: not in table",
        )

    def test_validate_values(self):
        with self.assertRaises(errors.InvalidValueError) as arc:
            u.validate_values({}, 31337)
        self.assertEqual(
            str(arc.exception), "invalid data in values_series[31337]: empty values"
        )

    def test_clean_values(self):
        dataset_good = ({Model.v_int: 1, Model.v_text.key: "kek"},)
        cleaned_values = u.clean_values(Model, dataset_good)
        self.assertSequenceEqual(cleaned_values, [{"v_int": 1, "v_text": "kek"}])

    def test_clean_values_empty_dataset(self):
        dataset_empty = []
        cleaned_values = u.clean_values(Model, dataset_empty)
        self.assertFalse(cleaned_values)

    def test_clean_values_fields_mismatch(self):
        dataset_fields_excess = [
            {Model.v_int: 1},
            {Model.v_int: 2, Model.v_text: "kek"},
        ]
        with self.assertRaises(errors.InvalidValueError) as arc:
            u.clean_values(Model, dataset_fields_excess)
        self.assertEqual(
            str(arc.exception),
            "invalid data in values_series[1]: keys mismatch: excess=['v_text'], missing=[]",
        )

        dataset_fields_missing = [
            {Model.v_int: 1, Model.v_text: "kek"},
            {Model.v_int: 1},
        ]
        with self.assertRaises(errors.InvalidValueError) as arc:
            u.clean_values(Model, dataset_fields_missing)
        self.assertEqual(
            str(arc.exception),
            "invalid data in values_series[1]: keys mismatch: excess=[], missing=['v_text']",
        )

        dataset_both = [{Model.v_int: 1}, {Model.v_text: "kek"}]
        with self.assertRaises(errors.InvalidValueError) as arc:
            u.clean_values(Model, dataset_both)
        self.assertEqual(
            str(arc.exception),
            "invalid data in values_series[1]: keys mismatch: excess=['v_text'], missing=['v_int']",
        )

    def test_clean_values_type_cast(self):
        dataset = [
            {
                Model.v_array.key: ["a", "b", "c"],
                Model.v_bool.key: True,
                Model.v_default.key: None,
                Model.v_float.key: 0.15,
                Model.v_int.key: 1,
                Model.v_numeric.key: Decimal("0.3"),
                Model.v_text.key: "'; drop database;",
            }
        ]

        values_set_trusted = u.clean_values(Model, dataset)
        self.assertEqual(len(values_set_trusted), 1)
        values_trusted = values_set_trusted[0]
        self.assertDictEqual(
            values_trusted,
            {
                "v_array": ["a", "b", "c"],
                "v_bool": True,
                "v_default": None,
                "v_float": 0.15,
                "v_int": 1,
                "v_numeric": Decimal("0.3"),
                "v_text": "'; drop database;",
            },
        )

        values_set_casted = u.clean_values(Model, dataset, cast_db_types=True)
        self.assertEqual(len(values_set_casted), 1)
        values_casted = values_set_casted[0]

        self.assertDictEqual(
            values_casted,
            {
                "v_array": "ARRAY['a','b','c']",
                "v_bool": 1,
                "v_default": "null",
                "v_float": 0.15,
                "v_int": 1,
                "v_numeric": "0.3",
                "v_text": "'''; drop database;'",
            },
        )

    def test_get_column_types(self):
        column_types = u.get_column_types(self.session, Model)
        self.assertDictEqual(
            column_types,
            {
                "id": "integer",
                "v_array": "text[]",
                "v_bool": "boolean",
                "v_date": "date",
                "v_datetime": "timestamp without time zone",
                "v_default": "integer",
                "v_float": "double precision",
                "v_int": "integer",
                "v_numeric": "numeric",
                "v_text": "text",
            },
        )

    def test_to_db_literal(self):
        scalar_casts = (
            (None, "null"),
            ("kek", "'kek'"),
            (True, 1),
            (False, 0),
            (0, 0),
            (0.0, 0.0),
            (Decimal("0.33"), "0.33"),
        )

        for original, cast_expect in scalar_casts:
            cast_got = u.to_db_literal(original)
            self.assertEqual(
                cast_expect,
                cast_got,
                f"to_db_literal({original}) = {cast_got} != {cast_expect}",
            )

        collection_casts = {
            "json": (
                (["a", "b"], repr('["a", "b"]')),
                (["a"], repr('["a"]')),
                ([1, 2], repr("[1, 2]")),
                ([1], repr("[1]")),
                ([], repr("[]")),
                ({"a": "b"}, repr('{"a": "b"}')),
                ({"a": 0}, repr('{"a": 0}')),
                ({"a": 1, "b": "c"}, repr('{"a": 1, "b": "c"}')),
                ({"a": 1}, repr('{"a": 1}')),
                ({"a": False}, repr('{"a": false}')),
                ({"a": None}, repr('{"a": null}')),
                ({"a": True}, repr('{"a": true}')),
                ({}, repr("{}")),
            ),
            "jsonb": (
                (["a", "b"], repr('["a", "b"]')),
                (["a"], repr('["a"]')),
                ([1, 2], repr("[1, 2]")),
                ([1], repr("[1]")),
                ([], repr("[]")),
                ({"a": "b"}, repr('{"a": "b"}')),
                ({"a": 0}, repr('{"a": 0}')),
                ({"a": 1, "b": "c"}, repr('{"a": 1, "b": "c"}')),
                ({"a": 1}, repr('{"a": 1}')),
                ({"a": False}, repr('{"a": false}')),
                ({"a": None}, repr('{"a": null}')),
                ({"a": True}, repr('{"a": true}')),
                ({}, repr("{}")),
            ),
            "hstore": (
                ({"a": "b"}, repr('"a"=>"b"')),
                ({"a": 0}, repr('"a"=>"0"')),
                ({"a": 1, "b": "c"}, repr('"a"=>"1","b"=>"c"')),
                ({"a": 1}, repr('"a"=>"1"')),
                ({"a": False}, repr('"a"=>"False"')),
                ({"a": None}, repr('"a"=>NULL')),
                ({"a": True}, repr('"a"=>"True"')),
                ({}, "''"),
            ),
            None: (
                (["a", "b"], "ARRAY['a','b']"),
                (["a"], "ARRAY['a']"),
                ([1, 2], "ARRAY[1,2]"),
                ([1], "ARRAY[1]"),
                ([], "'{}'"),
            ),
        }

        for cast_to, args in collection_casts.items():
            for original, cast_expect in args:
                cast_got = u.to_db_literal(original, cast_to=cast_to)
                self.assertEqual(
                    cast_expect,
                    cast_got,
                    f"to_db_literal({original!r}, cast_to={cast_to!r}) = {cast_got!r} != {cast_expect!r}",
                )

    def test_get_table_columns(self):
        expected = {
            "id",
            "v_array",
            "v_bool",
            "v_date",
            "v_datetime",
            "v_default",
            "v_float",
            "v_int",
            "v_numeric",
            "v_text",
        }
        got = u.get_table_columns(Model)
        self.assertSetEqual(expected, got)

    def test_get_column_key(self):
        columns = u.get_table_columns(Model)

        with self.assertRaises(errors.InvalidColumnError) as arc:
            u.get_column_key(Model, "x", None, None)
        self.assertEqual(
            str(arc.exception), "invalid key `x` in values_series: not in table"
        )

        with self.assertRaises(errors.InvalidColumnError) as arc:
            u.get_column_key(Model, "x", 88, None)
        self.assertEqual(
            str(arc.exception), "invalid key `x` in values_series[88]: not in table"
        )

        with self.assertRaises(errors.InvalidColumnError) as arc:
            u.get_column_key(Model, "id", None, frozenset({"a", "b"}))
        self.assertEqual(
            str(arc.exception), "invalid key `id` in values_series: not in table"
        )

        key = u.get_column_key(Model, Model.id)
        self.assertEqual(key, "id")

        key = u.get_column_key(Model, "id")
        self.assertEqual(key, "id")

        key = u.get_column_key(Model, Model.id, columns=columns)
        self.assertEqual(key, "id")

        key = u.get_column_key(Model, "id", columns=columns)
        self.assertEqual(key, "id")
