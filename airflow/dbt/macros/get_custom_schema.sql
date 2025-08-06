-- macros/get_custom_schema.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
      This macro overrides dbt's default schema generation.
      It ensures that if you specify a `+schema:` property in your dbt_project.yml,
      that value is used directly as the BigQuery dataset name, without being
      prefixed by the `dataset:` value from your profiles.yml.
    #}

    {%- if custom_schema_name is none -%}
        {# If no +schema: is defined, fall back to the default target schema #}
        {{ target.schema }}
    {%- else -%}
        {# Use the custom_schema_name (e.g., 'bronze', 'silver', 'gold') directly #}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}