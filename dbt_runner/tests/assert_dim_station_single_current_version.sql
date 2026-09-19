select
    station_id,
    count(*) as jumlah_current
from {{ ref('dim_station') }}
where is_current = true
group by station_id
having count(*) > 1