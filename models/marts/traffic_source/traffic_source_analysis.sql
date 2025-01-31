with base as (

    select * from {{ ref('stg_event__json_extract') }}

)

, event_steps as (

    select * from {{ ref('int_event_steps') }}

)

, final as (

    select
    traffic_medium
    , traffic_source
    , count(distinct b.user_pseudo_id) as cnt_users
    , count(case when funnel_step = 10 then b.user_pseudo_id end) as converted_users
    , round(count(case when funnel_step = 10 then b.user_pseudo_id end) * 1.0 / count(distinct b.user_pseudo_id) , 2) as user_conversion_rate
    from base as b
    join event_steps as e 
    on b.event_timestamp = e.event_timestamp
    where b.event_name IN ('view_item', 'add_shipping_info', 'add_payment_info')
    group by 1,2
    order by 1 desc
)

select * from final