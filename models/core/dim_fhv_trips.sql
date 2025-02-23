{{
    config( 
        materialized='table'
    )
}}

with fhv_tripdata as (
    select 
        *,
        EXTRACT(YEAR FROM pickup_datetime) as year_pi_datetime,
        EXTRACT(MONTH FROM pickup_datetime) as month_pi_datetime,

    from {{ ref('stg_staging__fhv_tripdata') }}
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
   fhv.*,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone_name,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone_name
from fhv_tripdata as fhv
inner join dim_zones as pickup_zone
on fhv.pickup_location_id = pickup_zone.zone_location_id
inner join dim_zones as dropoff_zone
on fhv.dropoff_location_id = dropoff_zone.zone_location_id