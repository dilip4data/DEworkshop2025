{{
    config(materialized = 'table')
}}

with taxi_monthly_fare as 
(
    select service_type, 
    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month,
    fare_amount from {{ ref('fact_trips') }}
     where fare_amount > 0 and trip_distance > 0 and 
      payment_type_description in {{ var("payment_desc") }}    

), percentiles as
    ( select service_type,year,month,
            PERCENTILE_CONT(fare_amount,0.97) OVER (PARTITION BY service_type, year, month ) as p97,
            PERCENTILE_CONT(fare_amount,0.95) OVER (PARTITION BY service_type, year, month ) as p95,
            PERCENTILE_CONT(fare_amount,0.90) OVER (PARTITION BY service_type, year, month ) as p90,
    from taxi_monthly_fare )

select * From percentiles;

-- dbt build --select +fct_taxi_trips_monthly_fare_p95.sql+ --vars '{is_test_run: false}'

-- SELECT distinct service_type,year,month, p97, p95, p90 FROM `dtcde-2025.dbt_dmangalk.fct_taxi_trips_monthly_fare_p95` where year = 2020 and month = 4


