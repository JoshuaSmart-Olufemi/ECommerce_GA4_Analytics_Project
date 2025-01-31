with base as (

    select * from {{ ref('stg_event__json_extract') }}

)

, most_viewed as (

    select
    page_title
    , count(*) as most_viewed_cnt
    from base 
    group by 1 
    order by 2 desc 
)

select * from most_viewed
where page_title is not null