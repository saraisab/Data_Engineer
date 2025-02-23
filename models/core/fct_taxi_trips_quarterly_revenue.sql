{{ config(materialized='table') }}

with quarterly_revenue as (
    SELECT 
    SUM(total_amount) AS revenue
    ,service_type
    ,EXTRACT(YEAR FROM pickup_datetime) as year_pi_datetime
    ,EXTRACT(QUARTER FROM pickup_datetime) as quarter_pi_datetime
    FROM 
        {{ ref('fact_trips') }}
        
    where
        EXTRACT(YEAR FROM pickup_datetime) in (2019, 2020)
    GROUP BY 
        service_type, 
        year_pi_datetime, 
        quarter_pi_datetime 
    order by 
        service_type, 
        quarter_pi_datetime, 
        year_pi_datetime
),
quarterly_growth as (
    select
        year_pi_datetime,
        quarter_pi_datetime,
        service_type, 
        revenue,
        LAG(revenue) OVER (partition by service_type, quarter_pi_datetime order by year_pi_datetime) as prev_year_revenue,
        (revenue - LAG(revenue) OVER (partition by service_type, quarter_pi_datetime order by year_pi_datetime))
        / NULLIF(LAG(revenue) OVER (partition by service_type, quarter_pi_datetime order by year_pi_datetime), 0) as yoy_growth

    from 
        quarterly_revenue 
    order by service_type, year_pi_datetime, quarter_pi_datetime

)
select * from quarterly_growth 
