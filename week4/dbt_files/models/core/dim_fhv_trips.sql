{{ config(materialized='table') }}

with fhvtrip as 
(
    select * From {{ ref('stg_fhv2019_tripdata') }}
), 
dimzones as
(
    select * from {{ ref("dim_zones") }} 
    where borough != 'Unknown'
), 
fhv_zones as 
(
    select fhv.dispatching_base_num,
            fhv.pickup_datetime,
            fhv.dropoff_datetime,
            extract(year from pickup_datetime) as year,
            extract(month from pickup_datetime) as month,
            fhv.PUlocationID,
            pickup_zone.borough as pickup_borough, 
            pickup_zone.zone as pickup_zone, 
            fhv.DOlocationID,
            dropoff_zone.borough as dropoff_borough, 
            dropoff_zone.zone as dropoff_zone, 
            fhv.sr_flag,
            fhv.affiliated_base_number
            
        from   fhvtrip fhv 
        inner join dimzones as pickup_zone 
            on fhv.PUlocationID = pickup_zone.locationid
        inner join dimzones as dropoff_zone
            on fhv.DOlocationID = dropoff_zone.locationid
            
) 
select * from fhv_zones