{{
    config(
        materialized='view'
    )
}}

with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,
        CAST(pickup_datetime as timestamp) as pickup_datetime,
        CAST(dropoff_datetime as timestamp) as dropoff_datetime,
        CAST(pulocationid as integer) as pickup_location_id,
        CAST(dolocationid as integer) as dropoff_location_id,
        sr_flag,
        affiliated_base_number

    from 
        source
    where
         dispatching_base_num is not null

)

select * from renamed
