select
    ride_id,
    started_at,
    ended_at,
    count(*) as jumlah
from {{ ref('fct_citibike_trips_enriched') }}
group by ride_id, started_at, ended_at
having count(*) > 1