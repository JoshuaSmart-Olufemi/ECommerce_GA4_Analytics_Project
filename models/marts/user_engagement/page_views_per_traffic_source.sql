with base as (

    select * from {{ ref('stg_event__json_extract') }}

)


, page_views as (

    select
    traffic_source_name
    , count(1) as pg_views_per_traffic_source_count
    from base 
    where event_name = 'page_view'
    group by 1
    order by 2 desc

)
select * from page_views