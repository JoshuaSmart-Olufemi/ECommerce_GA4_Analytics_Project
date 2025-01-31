with base as (

    select * from {{ ref('stg_event__json_extract') }}

)


, page_views as (

    select
    traffic_source_name
    , traffic_medium
    , count(1) as pg_views_per_traffic_source_count
    from base 
    where event_name = 'page_view'
    group by 1, 2
    order by 3 desc

)
select * from page_views