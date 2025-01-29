
{% macro remove_brackets(column_name) %}

    REGEXP_REPLACE({{ column_name }}, '[()]', '', 'g')

{% endmacro %}