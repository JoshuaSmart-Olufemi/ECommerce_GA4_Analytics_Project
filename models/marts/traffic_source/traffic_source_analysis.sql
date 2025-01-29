with base as (

    select * from {{ ref('stg_event__json_extract') }}

)

, traffic_sources as (

    select
    count(event_name) as page_view_cnt
    , traffic_medium
    , traffic_source
    from base 
    where event_name = 'page_view'
    group by 2,3
    order by 1 desc
)

select * from traffic_sources