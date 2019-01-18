from jinja2 import Template


class BulkOperationError(Exception):
    pass


class InvalidColumnError(BulkOperationError):
    def __init__(self, key, message="", index=None):
        tmpl = Template(
            'Invalid key "{{key}}" in values'
            "{% if index %}[{{index}}]{% endif %}"
            "{% if message %}: {{message}}{% endif %}"
        )

        msg = tmpl.render(key=key, message=message, index=index)

        super(self.__class__, self).__init__(msg)


class InvalidValueError(BulkOperationError):
    def __init__(self, index, message=""):
        tmpl = Template(
            "Invalid values[{{index}}]" "{% if message %}: {{message}}{% endif %}"
        )

        msg = tmpl.render(index=index, message=message)

        super(self.__class__, self).__init__(msg)
