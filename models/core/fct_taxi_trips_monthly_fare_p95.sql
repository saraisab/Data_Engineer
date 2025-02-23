{{ config(
    materialized='table'
    ) }}

with taxi_monthly_fare as (
    select
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) as year_pi_datetime,
        EXTRACT(MONTH FROM pickup_datetime) as month_pi_datetime,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(97)] AS percentile_97,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(95)] AS percentile_95,
        APPROX_QUANTILES(fare_amount, 100)[OFFSET(90)] AS percentile_90
    from
        {{ ref('fact_trips') }}
    where
        fare_amount > 0
        and trip_distance > 0
        and payment_type_description in ('Cash', 'Credit Card')
        and EXTRACT(YEAR FROM pickup_datetime) = 2020
        and EXTRACT(MONTH FROM pickup_datetime) = 4
    GROUP by
        service_type, 
        year_pi_datetime, 
        month_pi_datetime
) 
select * from taxi_monthly_fare
order by service_type, year_pi_datetime, month_pi_datetime