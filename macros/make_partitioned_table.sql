
{% macro new_records_for_date(date_field, date, sql, from_table) %}

    with source as (

        {{ sql }}

    )

    select *
    from source
    where {{ date_field }} = '{{ date }}'

{% endmacro %}

{% macro new_records_for_date_dedupe(date_field, date, sql, from_table, dest, unique_key) %}

    with source as (

        {{ sql }}

    ),

    dest as (

        select * from {{ dest }}

    )

    select *
    from source
    where {{ date_field }} = '{{ date }}'
      and {{ unique_key }} not in (select distinct {{ unique_key }} from dest)

{% endmacro %}

{% macro delete_records_for_date(date_field, date, sql, from_table) %}

    delete from {{ from_table }}
    where {{ date_field }} = '{{ date }}'
      and date_diff(current_date, {{ date_field }}, day) >= 2

{% endmacro %}

{% macro insert_into(dest_schema, dest_table, sql, columns) %}

    {% set columns_csv = columns | join(', ') %}

    insert into {{ dest_schema }}.{{ dest_table }} ({{ columns_csv }} )(

        with data as (

            {{ sql }}

        )

        select {{ columns_csv }} from data

    )


{% endmacro %}


{# -------------------- MATERIALIZATION --------------------  #}

{% materialization make_partitioned_table, default %}
    {%- set date_field = config.require('date_field') -%}
    {%- set from_table = config.get('from_table') -%}
    {%- set columns = config.get('columns') -%}
    {%- set unique_key = config.get('unique_key') -%}

    {% set source_schema, source_table = from_table.split(".") %}

    {%- set identifier = source_table ~ "_daily" %}

    {{ log(' -> Fetching existing tables in schema ' ~ source_schema ~ '...', info=True) }}
    {%- set existing_tables = adapter.query_for_existing(source_schema) -%}

    {{ log(' -> Fetching dates with data...', info=True) }}
    {% call statement('main', fetch_result=True) %}
        with data as (

            {{ sql }}

        )

        select distinct {{ date_field }} from data order by 1
    {% endcall %}

    {% set dates = load_result('main')['data'] | map(attribute=0) | list %}

    {{ log(dates) }}

    {% for date in dates %}
        {% set date_label = date | string | replace("-", "")  %}
        {% set period_identifier = identifier ~ date_label %}

        {% if period_identifier not in existing_tables %}
            {{ log(' -> Creating ' ~ period_identifier, info=True) }}

            {% set create_sql = bigquery.new_records_for_date(date_field, date, sql, from_table) %}
            {{ adapter.execute_model({"name": period_identifier, "injected_sql": create_sql, "schema": source_schema}, 'table') }}

        {% else %}
            {{ log(' -> Inserting into ' ~ period_identifier, info=True) }}
            {% set dest = source_schema ~ "." ~ period_identifier %}
            {% set insert_sql = bigquery.new_records_for_date_dedupe(date_field, date, sql, from_table, dest, unique_key) %}

            {% call statement() %}

                {{ bigquery.insert_into(source_schema, period_identifier, insert_sql, columns) }}

            {% endcall %}

        {% endif %}

        {% call statement() %}

            {{ bigquery.delete_records_for_date(date_field, date, sql, from_table) }}

        {% endcall %}
    {% endfor %}
{% endmaterialization %}
