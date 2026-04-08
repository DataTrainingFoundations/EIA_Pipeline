{% macro eia_list_to_sql(items) -%}
    {%- for item in items -%}
        '{{ item }}'{% if not loop.last %}, {% endif %}
    {%- endfor -%}
{%- endmacro %}


{% macro eia_var_or_env(var_name, env_name, default='') -%}
    {%- set value = var(var_name, none) -%}
    {%- if value is not none and value != '' -%}
        {{ return(value) }}
    {%- else -%}
        {{ return(env_var(env_name, default)) }}
    {%- endif -%}
{%- endmacro %}


{% macro eia_partition_dates() -%}
    {%- set partitions = var('partition_dates', none) -%}
    {%- if partitions is not none and partitions | length > 0 -%}
        {{ return(partitions) }}
    {%- endif -%}
    {{ return(fromjson(env_var('DBT_PARTITION_DATES_JSON', '[]'))) }}
{%- endmacro %}


{% macro eia_source_partition_predicate(source_name, table_name, column_name='partition_date', alias='') -%}
    {%- set partitions = eia_partition_dates() -%}
    {%- set start_date = eia_var_or_env('start_date', 'DBT_START_DATE', '') -%}
    {%- set end_date = eia_var_or_env('end_date', 'DBT_END_DATE', '') -%}
    {%- set qualifier = alias ~ '.' if alias else '' -%}
    {%- set column_ref = qualifier ~ column_name -%}
    {%- if partitions | length > 0 -%}
        {{ column_ref }} in ({{ eia_list_to_sql(partitions) }})
    {%- elif start_date or end_date -%}
        1 = 1
        {%- if start_date %} and {{ column_ref }} >= '{{ start_date }}'{% endif -%}
        {%- if end_date %} and {{ column_ref }} <= '{{ end_date }}'{% endif -%}
    {%- else -%}
        {{ column_ref }} = (select max({{ column_name }}) from {{ source(source_name, table_name) }})
    {%- endif -%}
{%- endmacro %}
