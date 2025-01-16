with 

source as (

    select * from {{ source('google_ga4', 'g4_events') }}

),

renamed as (

    select
        event_date
        , event_timestamp
        , event_name
        , nested_params
        , user_pseudo_id
        , user_first_touch_timestamp
        , traffic_source_medium
        , traffic_source_name
        , traffic_source_source

    from source

)

select * from renamed
