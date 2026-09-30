{#
  By default dbt names schemas "<target schema>_<custom schema>", e.g.
  staging_marts. This project wants exactly "staging" and "marts", because
  those names are part of the warehouse's permission model (see
  warehouse/00_roles_schemas.sql), so use the custom name as given.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
