select
    station_id,
    extracted_at,
    count(*) as jumlah
from {{ ref('stg_station_information') }}
group by station_id, extracted_at
having count(*) > 1