WITH event_steps AS (
-- Assigning step numbers to key funnel events

    SELECT 
        *
    FROM {{ ref('int_event_steps') }}
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
