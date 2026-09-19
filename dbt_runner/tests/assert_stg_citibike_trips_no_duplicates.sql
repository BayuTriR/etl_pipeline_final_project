select
    ride_id,
    started_at,
    ended_at,
    count(*) as jumlah
from {{ ref('stg_citibike_trips') }}
group by ride_id, started_at, ended_at
having count(*) > 1