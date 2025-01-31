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
),

-- Aggregating funnel data per session
funnel_aggregates AS (
    SELECT 
        ga_session_id,
        COUNT(DISTINCT user_pseudo_id) AS total_users,
        COUNT(DISTINCT CASE WHEN funnel_step = 5 THEN user_pseudo_id END) AS users_viewed_item,
        COUNT(DISTINCT CASE WHEN funnel_step = 9 THEN user_pseudo_id END) AS users_added_shipping,
        COUNT(DISTINCT CASE WHEN funnel_step = 10 THEN user_pseudo_id END) AS users_added_payment,
        
        -- Conversion rates
        CASE 
            WHEN COUNT(DISTINCT CASE WHEN funnel_step = 5 THEN user_pseudo_id END) = 0 
            THEN 0 
            ELSE COUNT(DISTINCT CASE WHEN funnel_step = 9 THEN user_pseudo_id END) 
                 * 1.0 / COUNT(DISTINCT CASE WHEN funnel_step = 5 THEN user_pseudo_id END)
        END AS shipping_conversion_rate,

        CASE 
            WHEN COUNT(DISTINCT CASE WHEN funnel_step = 9 THEN user_pseudo_id END) = 0 
            THEN 0 
            ELSE COUNT(DISTINCT CASE WHEN funnel_step = 10 THEN user_pseudo_id END) 
                 * 1.0 / COUNT(DISTINCT CASE WHEN funnel_step = 9 THEN user_pseudo_id END)
        END AS payment_conversion_rate,

        CASE 
            WHEN COUNT(DISTINCT CASE WHEN funnel_step = 5 THEN user_pseudo_id END) = 0 
            THEN 0 
            ELSE COUNT(DISTINCT CASE WHEN funnel_step = 10 THEN user_pseudo_id END) 
                 * 1.0 / COUNT(DISTINCT CASE WHEN funnel_step = 5 THEN user_pseudo_id END)
        END AS overall_conversion_rate

    FROM event_steps
    GROUP BY ga_session_id
)

SELECT * FROM funnel_aggregates
