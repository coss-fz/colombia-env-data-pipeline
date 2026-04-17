{# 
  Override dbt's default schema-naming so custom schemas in `+schema:` configs
  don't get concatenated onto the target dataset. Instead, `staging`, `marts`,
  and `intermediate` become sibling datasets alongside the base warehouse one.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
