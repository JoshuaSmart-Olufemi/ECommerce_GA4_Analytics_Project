with base as (

    select * from {{ ref('stg_event__json_extract') }}

)

, scroll_analysis as (

    select
    page_title
    , count(*) as scrolls_per_page_title_cnt
    from base 
    where event_name = 'scroll' and page_title is not null
    group by 1 
    order by 2 desc 
)

select * from scroll_analysis