{{ config(materialized='table') }}

with quarterly_revenue as (

    select service_type,
    extract(year from pickup_datetime) as year,
    extract(quarter from pickup_datetime)  as quarter,
    sum(total_amount) as revenue
    from {{ ref('fact_trips') }}
    where extract(year from pickup_datetime) in (2019,2020)
    group by service_type, year, quarter
), 
quarterly_growth as
(
    select year,quarter,service_type,revenue, lag(revenue) over (partition by service_type, quarter order by year) as prev_year_revenue,
    round((revenue - lag(revenue) over (partition by service_type, quarter order by year)) 
    / coalesce(lag(revenue) over (partition by service_type, quarter order by year),0) * 100,2) as YoY_growth
    from quarterly_revenue 
) select * From quarterly_growth 

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}