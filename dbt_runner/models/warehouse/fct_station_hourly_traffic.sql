{{
    config(
        materialized='table',
        partition_by={
            "field": "started_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by=["start_station_id", "member_casual"]
    )
}}

select
    started_date,
    started_hour,
    started_day_name,
    is_weekend,
    time_of_day,
    start_station_id,
    start_station_name,
    member_casual,
    rideable_type,

    count(ride_id) as total_trips,
    sum(trip_duration_seconds) as total_trip_duration_seconds,
    safe_divide(sum(trip_duration_seconds), count(ride_id)) as avg_trip_duration_seconds,

    count(case when data_quality_flag = 'valid' then 1 end) as valid_trips_count,
    count(case when data_quality_flag != 'valid' then 1 end) as flagged_trips_count

from {{ ref('fct_citibike_trips_enriched') }}
where start_station_id is not null
group by 1, 2, 3, 4, 5, 6, 7, 8, 9