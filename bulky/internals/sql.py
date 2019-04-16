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

STMT_GET_COLUMN_TYPES = """
SELECT
    c.column_name,
    (
        CASE c.data_type
        WHEN 'ARRAY'
            THEN e.data_type || '[]'
        WHEN 'USER-DEFINED'
            THEN c.udt_name
        ELSE c.data_type
        END
    ) AS column_type
    FROM information_schema.columns c
        LEFT JOIN information_schema.element_types e
            ON (
                (
                    c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    'TABLE',
                    c.dtd_identifier
                ) = (
                    e.object_catalog,
                    e.object_schema,
                    e.object_name,
                    e.object_type,
                    e.collection_type_identifier
                )
            )
    WHERE c.table_schema = 'public' AND c.table_name = '{{table_name}}'
    ORDER BY c.ordinal_position
    ;
"""
