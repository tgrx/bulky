from bulky import insert
from tests.db import *


class SimpleInsertTest(BulkyTest):
    def test_single_insert(self):
        dataset = tuple({Model.v_text: i} for i in range(1, 11))
        returning = (Model.id, Model.v_text)

        result = insert(self.session, Model, dataset, returning)

        result = {r.id: r.v_text for r in result}

        self.assertEqual(10, len(result))
        self.assertSequenceEqual([str(i) for i in range(1, 11)], tuple(result.values()))
