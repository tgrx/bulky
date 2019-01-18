STMT_INSERT = """
INSERT INTO {{table}}
    (
        {%- for column in columns -%}
            "{{column}}"
            {%- if not loop.last %}, {% endif %}
        {%- endfor -%}
    )

VALUES
    {% for values in values_list -%}
        (
            {%- for column in columns -%}
                {{values[column]}}
                {%- if not loop.last %}, {% endif -%}
            {%- endfor -%}
        )
        {%- if not loop.last %},{%- endif %}
    {% endfor %}
{% if returning -%}
RETURNING
    {% for column in returning -%}
    "{{column}}"
    {%- if not loop.last %}, {% endif -%}
    {%- endfor -%}
{%- endif %}
;
"""

STMT_UPDATE = """
WITH {{src}} (
    {% for column in columns -%}
    "{{column}}"{% if not loop.last %}, {% endif -%}
    {%- endfor %}
) AS (
    VALUES
    {%- for values in values_list %}
    (
        {%- for column in columns -%}
        {{values[column]}}
        {%- if not loop.last %}, {% endif -%}
        {%- endfor -%}
    )
    {%- if not loop.last %}, {% endif -%}
    {% endfor %}
)
UPDATE "{{dst}}"
SET
    {%- for column in columns_to_update %}
    "{{column}}" = "{{src}}"."{{column}}"::{{column_types[column]}}
    {%- if not loop.last %}, {% endif -%}
    {% endfor %}
FROM
    "{{src}}"
WHERE
    {%- for column in reference_fields %}
    "{{dst}}"."{{column}}" = "{{src}}"."{{column}}"::{{column_types[column]}}
    {%- if not loop.last %} AND {% endif -%}
    {% endfor %}
{% if update_changed -%}
    AND (
    {% for column in columns_to_update %}
        {%- if not loop.first %}OR {% endif -%}
        "{{dst}}"."{{column}}"
                <> "{{src}}"."{{column}}"::{{column_types[column]}}
        OR "{{dst}}"."{{column}}" IS NULL
        OR "{{src}}"."{{column}}" IS NULL
    {% endfor -%}
    )
{%- endif -%}
{% if returning %}
RETURNING
    {% for column in returning -%}
    "{{dst}}"."{{column}}"{%- if not loop.last %},{% endif -%}
    {% endfor %}
{% endif -%}
;
"""
