{{
    config( 
        materialized='view'
    )
}}

with trip_duration_fhv as (
    select
        DATETIME_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
        year_pi_datetime,
        month_pi_datetime,
        pickup_zone_name,
        dropoff_zone_name ,
        pickup_location_id,
        dropoff_location_id       
    from
        {{ ref('dim_fhv_trips') }}  
)
select
    year_pi_datetime,
    month_pi_datetime,
    pickup_zone_name,
    dropoff_zone_name,    
    PERCENTILE_CONT(trip_duration, 0.90) OVER (
        PARTITION BY 
            year_pi_datetime,
            month_pi_datetime,
            pickup_location_id,
            dropoff_location_id
    ) AS p90_trip_duration
from 
    trip_duration_fhv
where
    pickup_zone_name in ('Newark Airport', 'SoHo', 'Yorkville East')
    and month_pi_datetime = 11
    and year_pi_datetime = 2019
order by p90_trip_duration desc