from datetime import datetime
from decimal import Decimal
from typing import Union

from jinja2 import Template

from bulky import update
from .db import *


class UpdateTest(BulkyTest):
    def setUp(self):
        super().setUp()

        obj = Model()
        self.session.add(obj)
        self.session.flush()

        self.obj = obj

    def test_scalar_fields(self):
        dataset = {
            Model.id: self.obj.id,
            Model.v_array: None,
            Model.v_bool: None,
            Model.v_date: None,
            Model.v_datetime: None,
            Model.v_float: None,
            Model.v_int: None,
            Model.v_numeric: None,
            Model.v_text: None,
        }

        self.update_and_validate(dataset)

        dataset = {
            Model.id: self.obj.id,
            Model.v_array: ["A", "B", "C", "1", "2", "3"],
            Model.v_bool: True,
            Model.v_date: datetime.now().date(),
            Model.v_datetime: datetime.now(),
            Model.v_float: 3.14,
            Model.v_int: 1488,
            Model.v_numeric: Decimal(0.1),
            Model.v_text: "xyz",
        }

        self.update_and_validate(dataset)

    def test_multiple_references(self):
        dataset = {Model.id: self.obj.id, Model.v_int: 1, Model.v_text: "xxx"}
        self.update_and_validate(dataset)

        dataset = {Model.id: self.obj.id, Model.v_int: 1, Model.v_text: "yyy"}
        self.update_and_validate(dataset, references=[Model.id, Model.v_int])

    def test_returning(self):
        dataset = {Model.id: self.obj.id, Model.v_text: ":zzz%"}

        r = self.update_and_validate(dataset, returning=[Model.v_date, Model.v_text])

        self.assertTrue(r, "nothing returned")
        self.assertEqual(1, len(r), "returned result size mismatch")
        self.assertEqual(2, len(r[0]), "returned result fields mismatch")

        r = r[0]

        self.assertEqual(":zzz%", r.v_text, "string field mismatch")
        self.assertEqual(None, r.v_date, "date field mismatch")

    def update_and_validate(
        self, dataset, returning=None, references: Union[str, list] = "id"
    ):
        """
        Performs UPDATE and verifies that object is updated
        """

        r = update(
            self.session,
            Model,
            [dataset, dataset],
            returning=returning,
            reference_field=references,
        )

        self.session.refresh(self.obj)

        mismatches = []

        for attr, expected_value in dataset.items():
            key = attr.key
            got_value = getattr(self.obj, key)

            if isinstance(got_value, list):
                got_value = [
                    (str(elm) if isinstance(elm, str) else elm) for elm in got_value
                ]

            if str(expected_value) != str(got_value):
                mismatches.append((key, expected_value, got_value))

        tmpl = Template(
            "Update mismatch:\n"
            "{% for k, e, g in mismatches %}"
            "\t[{{k}}] expected {{e}}::{{type(e)}} != {{g}}::{{type(g)}}\n"
            "{% endfor %}\n"
        )

        self.assertFalse(mismatches, tmpl.render(mismatches=mismatches, type=type))

        return r
