
{% macro sql_for_date(date_source_field, date, sql, source_schema, source_relation, filter_output) %}

    {% set date_label = date | string | replace("-", "")  %}
    {% set source_relation_partition = source_relation ~ date_label %}

    with __dbt_source_data as (

        {% if source_schema and source_relation %}
            select * from {{ adapter.quote_schema_and_table(source_schema, source_relation_partition) }}
        {% else %}
            select 1 as __dbt_ignore
        {% endif %}

    ),

    data as (

        {{ sql }}

    )

    select *
    from data
    {% if filter_output %}
    where {{ date_source_field }} = '{{ date }}'
    {% endif %}

{% endmacro %}


{# -------------------- MATERIALIZATION --------------------  #}

{% materialization date_partitioned_table, default %}
    {%- set identifier = model['name'] %}

    {%- set date_source = config.require('date_source') -%}
    {%- set date_field = config.require('date_field') -%}
    {%- set filter_output = config.get('filter_output', True) -%}

    {%- set base_schema = config.get('date_partitioned_base_schema') -%}
    {%- set base_relation = config.get('date_partitioned_base_relation') -%}

    {{ log(' -> Fetching existing tables...', info=True) }}
    {%- set existing_tables = adapter.query_for_existing(schema) -%}

    {{ log(' -> Fetching dates with data...', info=True) }}
    {% call statement('main', fetch_result=True) %}
        select distinct {{ date_field }} from {{ ref(date_source) }} order by 1
    {% endcall %}

    {% set dates = load_result('main')['data'] | list %}

    {{ log(dates) }}

    {% for date_res in dates %}
        {% set date = date_res[0] %}
        {% set date_label = date | string | replace("-", "")  %}
        {% set period_identifier = identifier ~ date_label %}

        {% if flags.FULL_REFRESH or (period_identifier not in existing_tables) or loop.last %}
            {% set create_sql = sql_for_date(date_field, date, sql, base_schema, base_relation, filter_output) %}
            {{ log(' -> Running ' ~ period_identifier, info=True) }}
            {{ create_table_as(True, period_identifier, create_sql) }}
        {% else %}
            {{ log('Not running ' ~ period_identifier ~ ' -- already exists') }}
        {% endif %}
    {% endfor %}
{% endmaterialization %}
