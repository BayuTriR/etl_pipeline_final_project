{{
    config(
        materialized='table',
        partition_by={
            "field": "started_at",
            "data_type": "timestamp",
            "granularity": "day"
        },
        cluster_by=["start_station_id", "member_casual"]
    )
}}

with cleaned as (
    select 
        ingestion_at,
        source_month,
        source_data,
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        ROUND(
            CAST(TIMESTAMP_DIFF(ended_at, started_at, SECOND) AS NUMERIC) / 60.0, 2) 
        AS trip_duration_minutes,
        TIMESTAMP_DIFF(ended_at, started_at, second) as trip_duration_seconds,
        EXTRACT(DATE FROM started_at) AS started_date,
        EXTRACT(HOUR FROM started_at) AS started_hour,
        FORMAT_TIMESTAMP('%A', started_at) AS started_day_name,
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM started_at) IN (1, 7) THEN true 
            ELSE false 
        END AS is_weekend,
        CASE 
            WHEN EXTRACT(HOUR FROM started_at) BETWEEN 0 AND 5 THEN 'Late Night'
            WHEN EXTRACT(HOUR FROM started_at) BETWEEN 6 AND 10 THEN 'Morning'
            WHEN EXTRACT(HOUR FROM started_at) BETWEEN 11 AND 15 THEN 'Afternoon'
            WHEN EXTRACT(HOUR FROM started_at) BETWEEN 16 AND 18 THEN 'Evening'
            ELSE 'Night'
        END AS time_of_day,
        start_station_id,
        start_station_name,
        end_station_id,
        end_station_name,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual
    from {{ ref('stg_citibike_trips') }}
)

select
    *

from cleaned