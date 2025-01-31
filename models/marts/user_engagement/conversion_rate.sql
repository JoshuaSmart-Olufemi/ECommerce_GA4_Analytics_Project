with
    base as (
        
        select * from {{ ref("stg_event__json_extract") }}
        
        ),
    
    sessions_with_payment_info as (

    --- conversion rate : 0.94%  
      
        select 
        ga_session_id
        , max(case when event_name = 'add_payment_info' then 1 else 0 end) has_paid
        from base
        group by 1
        
        )
    
    select 
    concat( round(sum(has_paid)::numeric / count(ga_session_id) * 100 , 2) , '%' ) as conversion_rate
    from sessions_with_payment_info