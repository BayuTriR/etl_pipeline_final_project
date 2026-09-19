{{
    config(
        materialized='table'
    )
}}

with current_time_info as (
    select
        format_timestamp('%A', current_timestamp()) as current_day_name,
        extract(hour from current_timestamp()) as current_hour
),

historical_demand as (
    select
        start_station_id as short_name,
        started_hour,
        started_day_name,
        avg(total_trips) as avg_historical_trips
    from {{ ref('fct_station_hourly_traffic') }}
    group by start_station_id, started_hour, started_day_name
),

bridge as (
    select station_id, short_name
    from {{ ref('dim_station') }}
    where is_current = true
),

current_status as (
    select *
    from {{ ref('fct_station_status_current') }}
)

select
    cs.station_id,
    cs.station_name,
    cs.num_bikes_available,
    cs.num_docks_available,
    ct.current_hour,
    ct.current_day_name,
    hd.avg_historical_trips,
    case
        when cs.num_bikes_available = 0 then 'empty'
        when cs.num_docks_available = 0 then 'full'
        when hd.avg_historical_trips is null then 'no_historical_data'
        else 'normal'
    end as supply_status
from current_status cs
cross join current_time_info ct
left join bridge b
    on cs.station_id = b.station_id
left join historical_demand hd
    on b.short_name = hd.short_name
    and hd.started_hour = ct.current_hour
    and hd.started_day_name = ct.current_day_name