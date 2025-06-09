{{
  config(
    materialized='table',
    schema='audit'
  )
}}

-- Create schema if it doesn't exist
{% do run_query("CREATE SCHEMA IF NOT EXISTS audit") %}

-- Create or replace the audit table with all advanced fields
{% do run_query("
CREATE TABLE IF NOT EXISTS audit.dbt_runs (
  run_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  duration_seconds DOUBLE PRECISION,
  models_updated TEXT[],
  rows_affected JSONB,
  error_message TEXT,
  timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  dbt_version TEXT,
  target_name TEXT,
  start_time TIMESTAMPTZ,
  end_time TIMESTAMPTZ
)") %}

SELECT 'Audit schema and table created/verified' as audit_status