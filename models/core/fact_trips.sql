{{
    config( 
        materialized='table'
    )
}}

with green_tripdata as (
    select *,
        'Green' as service_type
    from {{ ref('stg_staging__green_tripdata') }}
),
yellow_tripdata as (
    select *,
        'Yellow' as service_type
    from {{ ref('stg_staging__yellow_tripdata') }}
),
trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),
dim_zones as (
    select 
        locationid as zone_location_id, 
        borough, 
        zone 
    from {{ ref('dim_zones') }}
    -- para que no salgan los valores unknown de borough
    where borough != 'Unknown'
)

select 
    trips_unioned.*, 
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone_name,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone_name
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_location_id = pickup_zone.zone_location_id
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_location_id = dropoff_zone.zone_location_id