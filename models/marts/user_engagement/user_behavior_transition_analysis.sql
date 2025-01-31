with base as (

    select * from {{ ref('int_event_steps') }}

)

, inter as (

    select
    user_pseudo_id
    , event_name
    , ga_session_id
    , event_timestamp 
    , lag(event_name) over (partition by user_pseudo_id order by event_timestamp) as previous_event
    , lead(event_name) over (partition by user_pseudo_id order by event_timestamp) as next_event
    from base 
    

)

select 
previous_event
, event_name
, next_event
, count(*) as transition_count
from inter
where previous_event is not null and event_name is not null and next_event is not null
group by 1,2,3
order by 4 desc