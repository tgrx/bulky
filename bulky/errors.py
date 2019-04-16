class BulkOperationError(Exception):
    pass


class InvalidColumnError(BulkOperationError):
    def __init__(self, key, message="", index=None):
        e = f"invalid key `{key}` in values_series"
        if index is not None:
            e += f"[{index}]"
        if message:
            e += f": {message}"

        super().__init__(e)


class InvalidValueError(BulkOperationError):
    def __init__(self, index, message=""):
        e = f"invalid data in values_series[{index}]"
        if message:
            e += f": {message}"

        super().__init__(e)
