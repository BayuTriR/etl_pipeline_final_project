{{
    config(
        materialized='view'
    )
}}

select
    station_id,
    station_name,
    short_name,
    lat,
    lon,
    region_id,
    capacity,
    rental_uri_android,
    rental_uri_ios,
    extracted_at
from {{ source('batch_staging', 'gbfs_station_information') }}