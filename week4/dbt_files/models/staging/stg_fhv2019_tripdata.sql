{{
    config(
        materialized='view'
    )
}}

with fhv2019 as 
(
    select dispatching_base_num,
            cast(pickup_datetime as timestamp) as pickup_datetime,
            cast(drop_off_datetime as timestamp) as dropoff_datetime,
            pu_location_id as PUlocationID,
            do_location_id as DOlocationID,
            sr_flag,
            affiliated_base_number
     From {{ source('staging', 'fhv_trips') }} 
    where dispatching_base_num is not null
) 
select * from fhv2019 

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
