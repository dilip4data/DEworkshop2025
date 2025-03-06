{{
    config(
        materialized='view'
    )
}}

select *
from {{ ref('fact_trips') }}
where  
pickup_datetime >= cast('2021-02-02 10:08:51' as timestamp) - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

-- cast('2021-02-02 10:08:51' as timestamp) - INTERVAL '30' DAY

