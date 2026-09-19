{{
    config(
        materialized='table'
    )
}}

with base as (
    select
        station_id,
        station_name,
        short_name,
        lon,
        lat,
        region_id,
        capacity,
        rental_uri_android,
        rental_uri_ios,
        extracted_at as valid_from
    from {{ source('batch_staging', 'gbfs_station_information') }}
),

with_change_detection as (
    select
        *,
        lag(station_name) over (partition by station_id order by valid_from) as prev_station_name,
        lag(short_name) over (partition by station_id order by valid_from) as prev_short_name,
        lag(lon) over (partition by station_id order by valid_from) as prev_lon,
        lag(lat) over (partition by station_id order by valid_from) as prev_lat,
        lag(region_id) over (partition by station_id order by valid_from) as prev_region_id,
        lag(capacity) over (partition by station_id order by valid_from) as prev_capacity
    from base
),

only_changes as (
    select *
    from with_change_detection
    where
        prev_station_name is null
        or station_name != prev_station_name
        or short_name != prev_short_name
        or lon != prev_lon
        or lat != prev_lat
        or region_id != prev_region_id
        or capacity != prev_capacity
),

with_next_date as (
    select
        * except(prev_station_name, prev_short_name, prev_lon, prev_lat, prev_region_id, prev_capacity),
        lead(valid_from) over (partition by station_id order by valid_from) as valid_to
    from only_changes
)

select
    to_hex(md5(concat(cast(station_id as string), cast(valid_from as string)))) as station_sk,
    station_id,
    station_name,
    short_name,
    lon,
    lat,
    region_id,
    capacity,
    rental_uri_android,
    rental_uri_ios,
    valid_from,
    coalesce(valid_to, timestamp('9999-12-31 23:59:59')) as valid_to,
    case when valid_to is null then true else false end as is_current

from with_next_date