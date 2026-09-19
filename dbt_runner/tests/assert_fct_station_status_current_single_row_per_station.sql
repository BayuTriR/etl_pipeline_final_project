select
    station_id,
    count(*) as jumlah
from {{ ref('fct_station_status_current') }}
group by station_id
having count(*) > 1