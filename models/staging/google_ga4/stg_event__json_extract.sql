with 

source as (

    select * from {{ source('google_ga4', 'g4_events') }}

),

renamed as (

    select
        event_date
        , event_timestamp
        , event_name
        , user_pseudo_id
        , nested_params
        , user_first_touch_timestamp
        , traffic_source_medium
        , traffic_source_name
        , traffic_source_source
        , revenue

    from source

)
,
cleaned_data AS (
    SELECT
        *,
        -- Replace NaN values with null in the JSON object
        CASE
            WHEN nested_params LIKE '%NaN%'
            THEN REPLACE(nested_params, 'NaN', 'null')::jsonb
            ELSE nested_params::jsonb
        END AS cleaned_params
    FROM renamed
),
expanded_params AS (
    SELECT
        *,
        jsonb_array_elements(cleaned_params->'event_params') AS param
    FROM cleaned_data
),

extracted_fields AS (
    SELECT
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        -- Extract values from the param JSON object
        CASE WHEN param->>'event_params.key' = 'ga_session_number'
             THEN (param->>'event_params.value.int_value')::FLOAT
        END AS ga_session_number,
        CASE WHEN param->>'event_params.key' = 'ga_session_id'
             THEN (param->>'event_params.value.int_value')::FLOAT
        END AS ga_session_id,
        CASE WHEN param->>'event_params.key' = 'page_title'
             THEN param->>'event_params.value.string_value'
        END AS page_title,
        CASE WHEN param->>'event_params.key' = 'percent_scrolled'
             THEN (param->>'event_params.value.int_value')::FLOAT
        END AS percent_scrolled,
        CASE WHEN param->>'event_params.key' = 'debug_mode'
             THEN (param->>'event_params.value.int_value')::FLOAT
        END AS debug_mode,
        user_first_touch_timestamp,
        traffic_source_medium,
        traffic_source_name,
        traffic_source_source,
        revenue
    FROM expanded_params
)

--select * from extracted_fields

,
aggregated_fields AS (
    SELECT
        event_date,
        event_timestamp,
        event_name,
        user_pseudo_id,
        MAX(ga_session_number) AS ga_session_number,
        MAX(ga_session_id) AS ga_session_id,
        MAX(page_title) AS page_title,
        MAX(percent_scrolled) AS percent_scrolled,
        MAX(debug_mode) AS debug_mode,
        user_first_touch_timestamp,
        case when traffic_source_medium = '(none)' then null else traffic_source_medium end as traffic_source_medium,
        traffic_source_name,
        traffic_source_source,
        revenue
    FROM extracted_fields
    GROUP BY event_date, event_timestamp, event_name, user_pseudo_id, user_first_touch_timestamp,
        traffic_source_medium,
        traffic_source_name,
        traffic_source_source,
        revenue
)

SELECT * 
FROM aggregated_fields
--where page_title is not null
--user_pseudo_id = 1173058.1267899976    --4655360.668951162
order by user_pseudo_id

 

  