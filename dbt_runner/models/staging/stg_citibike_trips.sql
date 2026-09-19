{{
    config(
        materialized='view'
    )
}}

select
    ingestion_at,
    source_month,
    source_data,
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual
from {{ source('batch_staging', 'citibike_trips') }}
qualify row_number() over (
    partition by ride_id, started_at, ended_at
    order by
        case when cast(source_month as string) = format_date('%Y%m', date(started_at)) then 0 else 1 end,
        ingestion_at desc
) = 1