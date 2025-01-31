WITH raw_events AS (
    SELECT 
        *
    FROM {{ ref('stg_event__json_extract') }}
),

-- Assigning step numbers to key funnel events
event_steps AS (
    SELECT 
        user_pseudo_id,
        ga_session_id,
        event_timestamp,
        event_name,
        CASE 
            WHEN event_name = 'session_start' THEN 1
            WHEN event_name = 'first_visit' THEN 2
            WHEN event_name = 'page_view' THEN 3
            WHEN event_name = 'view_search_results' THEN 4
            WHEN event_name = 'view_item' THEN 5
            WHEN event_name = 'view_promotion' THEN 6
            WHEN event_name = 'select_promotion' THEN 7
            WHEN event_name = 'click' THEN 8
            WHEN event_name = 'add_shipping_info' THEN 9
            WHEN event_name = 'add_payment_info' THEN 10
            WHEN event_name = 'user_engagement' THEN 11
            WHEN event_name = 'scroll' THEN 12
        END AS funnel_step
    FROM raw_events
)

select * from event_steps 