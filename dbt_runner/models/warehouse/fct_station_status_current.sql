{{
    config(
        materialized='table'
    )
}}

with latest_status as (
    select *
    from {{ ref('stg_station_status_streaming') }}
    qualify row_number() over (
        partition by station_id
        order by ingestion_at desc
    ) = 1
),

dim_station_current as (
    select
        station_id,
        station_name,
        short_name,
        lat,
        lon
    from {{ ref('dim_station') }}
    where is_current = true
)

select
    s.ingestion_at,
    s.station_id,
    d.station_name,
    d.short_name,
    d.lat,
    d.lon,
    s.num_bikes_available,
    s.num_bikes_disabled,
    s.num_docks_available,
    s.num_docks_disabled,
    s.num_ebikes_available,
    s.is_installed,
    s.is_renting,
    s.is_returning,
    s.last_reported
from latest_status s
left join dim_station_current d
    on s.station_id = d.station_id