with base as (

    select * from {{ ref('stg_event__json_extract') }}

)

, count_actions as (

    select
    event_date
    , count( case when event_name = 'view_promotion' then 1 end) as count_viewed
    , count( case when event_name = 'select_promotion' then 1 end) as count_selected
    , count( case when event_name = 'scroll' then 1 end) as count_scroll
    , count( case when event_name = 'user_engagement' then 1 end) as count_user_engagement
    from base 
    group by 1 
)

, final as (

-----     click_through rate:    0.05%  
-----     view_rate:             0.02%
-----     user_engangement_rate: 1.59%

    select 
    concat(round(count_selected::numeric / count_viewed:: numeric , 2) , '%' ) as click_through_rate
    , concat(round(count_viewed::numeric / count(b.event_date) ,2) , '%' ) as view_rate
    , concat(round(count_user_engagement::numeric / count_scroll::numeric , 2) , '%') as user_engagement_rate
    from count_actions as ca
    join 
    base as b
    on b.event_date = ca.event_date
    group by ca.count_viewed, ca.count_selected,ca.count_user_engagement, ca.count_scroll

)
select * from final
