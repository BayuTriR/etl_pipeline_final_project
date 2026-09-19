select
    station_id,
    count(*) as jumlah
from {{ ref('fct_station_demand_vs_supply') }}
group by station_id
having count(*) > 1