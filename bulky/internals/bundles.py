from collections import namedtuple

InsertRenderBundle = namedtuple(
    "InsertRenderBundle",
    ("column_types", "columns", "returning", "table", "values_list"),
)

InsertExecuteBundle = namedtuple(
    "InsertExecuteBundle", ("queries", "result", "returning", "session")
)

UpdateRenderBundle = namedtuple(
    "UpdateRenderBundle",
    (
        "column_types",
        "columns",
        "columns_to_update",
        "reference_fields",
        "returning",
        "table",
        "values_list",
    ),
)
