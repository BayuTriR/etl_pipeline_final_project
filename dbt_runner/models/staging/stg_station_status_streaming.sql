{{
    config(
        materialized='view'
    )
}}

select
    cast(ingestion_at as timestamp) as ingestion_at,
    timestamp_seconds(feed_last_updated) as feed_last_updated,
    station_id,
    num_bikes_available,
    num_bikes_disabled,
    num_docks_available,
    num_docks_disabled,
    is_installed,
    is_renting,
    is_returning,
    cast(last_reported as timestamp) as last_reported,
    num_ebikes_available,
    num_scooters_available,
    num_scooters_unavailable
from {{ source('batch_staging', 'station_status_streaming') }}