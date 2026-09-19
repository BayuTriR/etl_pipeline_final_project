select
    started_date,
    started_hour,
    start_station_id,
    member_casual,
    rideable_type,
    count(*) as jumlah
from {{ ref('fct_station_hourly_traffic') }}
group by started_date, started_hour, start_station_id, member_casual, rideable_type
having count(*) > 1