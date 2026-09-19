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

with trips as (
    select * from {{ ref('int_citibike_trips') }}
),

dim_station as (
    select
        station_id,
        station_name,
        short_name,
        lat,
        lon,
        valid_from,
        valid_to
    from {{ ref('dim_station') }}
    where is_current = true
),

enriched as (

    select
        t.ingestion_at,
        t.source_month,
        t.source_data,
        t.ride_id,
        t.rideable_type,
        t.started_at,
        t.ended_at,
        t.trip_duration_seconds,
        t.trip_duration_minutes,
        t.started_date,
        t.started_hour,
        t.started_day_name,
        t.is_weekend,
        t.time_of_day,
        t.member_casual,

        coalesce(t.start_station_id, ds_start_id.short_name, ds_start_name.short_name) as start_station_id,
        coalesce(t.start_station_name, ds_start_id.station_name, ds_start_name.station_name) as start_station_name,
        coalesce(t.start_lat, ds_start_id.lat, ds_start_name.lat) as start_lat,
        coalesce(t.start_lng, ds_start_id.lon, ds_start_name.lon) as start_lng,

        coalesce(t.end_station_id, ds_end_id.short_name, ds_end_name.short_name) as end_station_id,
        coalesce(t.end_station_name, ds_end_id.station_name, ds_end_name.station_name) as end_station_name,
        coalesce(t.end_lat, ds_end_id.lat, ds_end_name.lat) as end_lat,
        coalesce(t.end_lng, ds_end_id.lon, ds_end_name.lon) as end_lng

    from trips t
    left join dim_station ds_start_id
        on t.start_station_id = ds_start_id.short_name
    left join dim_station ds_end_id
        on t.end_station_id = ds_end_id.short_name
    left join dim_station ds_start_name
        on t.start_station_id is null
        and t.start_station_name = ds_start_name.station_name
    left join dim_station ds_end_name
        on t.end_station_id is null
        and t.end_station_name = ds_end_name.station_name
)

select
    ingestion_at,
    source_month,
    source_data,
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    trip_duration_seconds,
    trip_duration_minutes,
    started_date,
    started_hour,
    started_day_name,
    is_weekend,
    time_of_day,
    start_station_id,
    start_station_name,
    end_station_id,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    case
        when start_station_id is null or start_lat is null or start_lng is null then 'missing_start_data'
        when end_station_id is null or end_lat is null or end_lng is null then 'missing_end_data'
        when trip_duration_seconds < 0 then 'negative_duration'
        when trip_duration_seconds = 0 then 'zero_duration'
        when trip_duration_seconds > 86400 then 'excessive_duration'
        else 'valid'
    end as data_quality_flag

from enriched