## Module 4 Homework - DE Zoomcamp
With the data in BigQuery


### Question 1: Understanding dbt model resolution

Provided you've got the following sources.yaml
```yaml
version: 2

sources:
  - name: raw_nyc_tripdata
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"
    tables:
      - name: ext_green_taxi
      - name: ext_yellow_taxi
```

with the following env variables setup where `dbt` runs:
```shell
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

What does this .sql model compile to?
```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```

- `select * from dtc_zoomcamp_2025.raw_nyc_tripdata.ext_green_taxi`
- `select * from dtc_zoomcamp_2025.my_nyc_tripdata.ext_green_taxi`
- `select * from myproject.raw_nyc_tripdata.ext_green_taxi`
- `select * from myproject.my_nyc_tripdata.ext_green_taxi`
- `select * from dtc_zoomcamp_2025.raw_nyc_tripdata.green_taxi`


Solution : 

`select * from dtc_zoomcamp_2025.raw_nyc_tripdata.ext_green_taxi`

---

### Question 2: dbt Variables & Dynamic Models

Say you have to modify the following dbt_model (`fct_recent_taxi_trips.sql`) to enable Analytics Engineers to dynamically control the date range. 

- In development, you want to process only **the last 7 days of trips**
- In production, you need to process **the last 30 days** for analytics

```sql
select *
from {{ ref('fact_taxi_trips') }}
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY
```

What would you change to accomplish that in a such way that command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?

- Add `ORDER BY pickup_datetime DESC` and `LIMIT {{ var("days_back", 30) }}`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", 30) }}' DAY`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", "30") }}' DAY`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY`
- Update the WHERE clause to `pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY`

Solution:
`Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY`


### Question 3: dbt Data Lineage and Execution

Considering the data lineage below **and** that taxi_zone_lookup is the **only** materialization build (from a .csv seed file):

![image](./homework_q2.png)

Select the option that does **NOT** apply for materializing `fct_taxi_monthly_zone_revenue`:

- `dbt run`
- `dbt run --select +models/core/dim_taxi_trips.sql+ --target prod`
- `dbt run --select +models/core/fct_taxi_monthly_zone_revenue.sql`
- `dbt run --select +models/core/`
- `dbt run --select models/staging/+`


Solution:

`dbt run --select models/staging/+`

Because the fct_taxi_monthly_zone_revenue is not considered as a staging model

---
### Question 4: dbt Macros and Jinja

Consider you're dealing with sensitive data (e.g.: [PII](https://en.wikipedia.org/wiki/Personal_data)), that is **only available to your team and very selected few individuals**, in the `raw layer` of your DWH (e.g: a specific BigQuery dataset or PostgreSQL schema), 

 - Among other things, you decide to obfuscate/masquerade that data through your staging models, and make it available in a different schema (a `staging layer`) for other Data/Analytics Engineers to explore

- And **optionally**, yet  another layer (`service layer`), where you'll build your dimension (`dim_`) and fact (`fct_`) tables (assuming the [Star Schema dimensional modeling](https://www.databricks.com/glossary/star-schema)) for Dashboarding and for Tech Product Owners/Managers

You decide to make a macro to wrap a logic around it:

```sql
{% macro resolve_schema_for(model_type) -%}

    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET'  -%}
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}

    {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}
    {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}
    {%- endif -%}

{%- endmacro %}
```

And use on your staging, dim_ and fact_ models as:
```sql
{{ config(
    schema=resolve_schema_for('core'), 
) }}
```

That all being said, regarding macro above, **select all statements that are true to the models using it**:
- Setting a value for  `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile
- Setting a value for `DBT_BIGQUERY_STAGING_DATASET` env var is mandatory, or it'll fail to compile
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET`
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET`


Solution:
- Setting a value for  `DBT_BIGQUERY_TARGET_DATASET` env var is mandatory, or it'll fail to compile *TRUE*
- When using `core`, it materializes in the dataset defined in `DBT_BIGQUERY_TARGET_DATASET` *TRUE*
- When using `stg`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET` *TRUE*
- When using `staging`, it materializes in the dataset defined in `DBT_BIGQUERY_STAGING_DATASET`, or defaults to `DBT_BIGQUERY_TARGET_DATASET` *TRUE*
---
## Serious SQL

Alright, in module 1, you had a SQL refresher, so now let's build on top of that with some serious SQL.

These are not meant to be easy - but they'll boost your SQL and Analytics skills to the next level.  
So, without any further do, let's get started...

You might want to add some new dimensions `year` (e.g.: 2019, 2020), `quarter` (1, 2, 3, 4), `year_quarter` (e.g.: `2019/Q1`, `2019-Q2`), and `month` (e.g.: 1, 2, ..., 12), **extracted from pickup_datetime**, to your `fct_taxi_trips` OR `dim_taxi_trips.sql` models to facilitate filtering your queries


### Question 5: Taxi Quarterly Revenue Growth

1. Create a new model `fct_taxi_trips_quarterly_revenue.sql`
2. Compute the Quarterly Revenues for each year for based on `total_amount`
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
  * e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
  * e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow

- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q3, worst: 2020/Q4}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q3, worst: 2020/Q4}

Solution:

green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

Model fct_taxi_quarterly_revenue.sql
```sql
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
```

---


### Question 6: P97/P95/P90 Taxi Monthly Fare

1. Create a new model `fct_taxi_trips_monthly_fare_p95.sql`
2. Filter out invalid entries (`fare_amount > 0`, `trip_distance > 0`, and `payment_type_description in ('Cash', 'Credit Card')`)
3. Compute the **continous percentile** of `fare_amount` partitioning by service_type, year and and month

Now, what are the values of `p97`, `p95`, `p90` for Green Taxi and Yellow Taxi, in April 2020?

- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5}
- green: {p97: 40.0, p95: 33.0, p90: 24.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}
- green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 25.5, p90: 19.0}

Solution:

* green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

As I downloaded the data from the bigquery public datasets, the payment_type column in green service was in float, and in yellow was in integer, so I had to transform the data with this query:
```sql
UPDATE  `celtic-surface-447817-d0.dataset_dbt_zoomcamp.green_tripdata`
SET payment_type = left(payment_type, 1)
where payment_type is not null
```

This is the model that I've created:
```sql
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

select 
    * 
from 
    taxi_monthly_fare
order by 
    service_type, 
    year_pi_datetime, 
    month_pi_datetime
```

### Question 7: Top #Nth longest P90 travel time Location for FHV

Prerequisites:
* Create a staging model for FHV Data (2019), and **DO NOT** add a deduplication step, just filter out the entries where `where dispatching_base_num is not null`
* Create a core model for FHV Data (`dim_fhv_trips.sql`) joining with `dim_zones`. Similar to what has been done [here](../../../04-analytics-engineering/taxi_rides_ny/models/core/fact_trips.sql)
* Add some new dimensions `year` (e.g.: 2019) and `month` (e.g.: 1, 2, ..., 12), based on `pickup_datetime`, to the core model to facilitate filtering for your queries

Now...
1. Create a new model `fct_fhv_monthly_zone_traveltime_p90.sql`
2. For each record in `dim_fhv_trips.sql`, compute the [timestamp_diff](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp_diff) in seconds between dropoff_datetime and pickup_datetime - we'll call it `trip_duration` for this exercise
3. Compute the **continous** `p90` of `trip_duration` partitioning by year, month, pickup_location_id, and dropoff_location_id

For the Trips that **respectively** started from `Newark Airport`, `SoHo`, and `Yorkville East`, in November 2019, what are **dropoff_zones** with the 2nd longest p90 trip_duration ?

- LaGuardia Airport, Chinatown, Garment District
- LaGuardia Airport, Park Slope, Clinton East
- LaGuardia Airport, Saint Albans, Howard Beach
- LaGuardia Airport, Rosedale, Bath Beach
- LaGuardia Airport, Yorkville East, Greenpoint

Solution:

- LaGuardia Airport, Chinatown, Garment District

In order to resolve that questions I've had to complete some models.

1. I've modified the schema and adding this:
```yml
tables:
      - name: green_tripdata
      - name: yellow_tripdata
      - name: fhv_tripdata

models:
  - name: stg_staging__fhv_tripdata
    description: ""
    columns:
      - name: dispatching_base_num
        data_type: string
        description: ""

      - name: pick_up_datetime
        data_type: string
        description: ""

      - name: dropOff_datetime
        data_type: string
        description: ""

      - name: PUlocationID
        data_type: numeric
        description: ""

      - name: DOlocationID
        data_type: numeric
        description: ""
        
      - name: SR_Flag
        data_type: numeric
        description: ""

      - name: Affiliated_base_number
        data_type: string
        description: ""
```
2. I've added the staging model
```sql
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
```
3. I've created a new core model `dim_fhv_trips.sql`
```sql
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
```
4. I've created a new core model `fct_fhv_monthly_zone_traveltime_p90.sql`
```sql
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
```
