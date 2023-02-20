{{ config(materialized='view') }}

-- select * from trips_data_all.fhv_tripdata
select 
    -- identifiers
    dispatching_base_num,
    cast(PUlocationID as integer) as  pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    SR_Flag,
    Affiliated_base_number,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
from {{ source('staging','fhv_tripdata') }}


{% if var('is_test_run', default = true) %}
    limit 100
{% endif %}