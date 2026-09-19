select
    data_quality_flag,
    count(*) as jumlah,
    round(count(*) * 100.0 / sum(count(*)) over (), 2) as persentase
from {{ ref('int_citibike_trips') }}
group by data_quality_flag
order by jumlah desc