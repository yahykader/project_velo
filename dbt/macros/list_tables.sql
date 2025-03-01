-- list all tables from the same dataset
{% macro list_tables(dataset_name) %}
    {% set query %}
        SELECT table_name
        FROM `{{ dataset_name }}.INFORMATION_SCHEMA.TABLES`
        WHERE REGEXP_CONTAINS(table_name, '[0-9]') = true
        ORDER BY table_name DESC
    {% endset %}

    {% set results = run_query(query) %}
    {{ return(results) }}
{% endmacro %}
