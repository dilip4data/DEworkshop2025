{{ config(materialized='table') }}

with tripduration as
(

    select dispatching_base_num,
            pickup_datetime,
            dropoff_datetime,
            TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) as trip_duration,
            year,
            month,
            PUlocationID,
            pickup_borough, 
            pickup_zone, 
            DOlocationID,
            dropoff_borough, 
            dropoff_zone, 
            sr_flag,
            affiliated_base_number 
            from {{ ref('dim_fhv_trips') }}
)
select tripduration.*, 
percentile_cont(trip_duration,0.9) over (PARTITION BY year, month, PUlocationID, DOlocationID) as tripduration_p90 
From tripduration
