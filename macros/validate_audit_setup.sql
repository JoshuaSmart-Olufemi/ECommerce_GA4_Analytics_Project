{% macro validate_audit_setup(schema, table) %}
  {% set check_query %}
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.tables
      WHERE table_schema = '{{ schema }}'
      AND table_name = '{{ table }}'
    );
  {% endset %}
 
  {% set exists = run_query(check_query).rows[0][0] %}
  {{ log("Audit table exists: " ~ exists, info=true) }}
{% endmacro %}

