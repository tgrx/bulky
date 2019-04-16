import unittest

from bulky import errors


class ErrorsTest(unittest.TestCase):
    longMessage = True

    def test_error_messages_rendering(self):
        self.assertEqual(
            str(errors.BulkOperationError("kek")),
            "kek",
            f"failed to render message for {errors.BulkOperationError}",
        )

        self.assertEqual(
            str(errors.InvalidColumnError("key")),
            "invalid key `key` in values_series",
            f"failed to render message for {errors.InvalidColumnError}",
        )

        self.assertEqual(
            str(errors.InvalidColumnError("key", "reason")),
            "invalid key `key` in values_series: reason",
            f"failed to render message for {errors.InvalidColumnError}",
        )

        self.assertEqual(
            str(errors.InvalidColumnError("key", "reason", 31337)),
            "invalid key `key` in values_series[31337]: reason",
            f"failed to render message for {errors.InvalidColumnError}",
        )

        self.assertEqual(
            str(errors.InvalidColumnError("key", index=31337)),
            "invalid key `key` in values_series[31337]",
            f"failed to render message for {errors.InvalidColumnError}",
        )

        self.assertEqual(
            str(errors.InvalidValueError(31337)),
            "invalid data in values_series[31337]",
            f"failed to render message for {errors.InvalidValueError}",
        )

        self.assertEqual(
            str(errors.InvalidValueError(31337, "reason")),
            "invalid data in values_series[31337]: reason",
            f"failed to render message for {errors.InvalidValueError}",
        )
