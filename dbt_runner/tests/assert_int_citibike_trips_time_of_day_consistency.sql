select
    ride_id,
    started_hour,
    time_of_day
from {{ ref('int_citibike_trips') }}
where
    (started_hour between 0 and 5 and time_of_day != 'Late Night')
    or (started_hour between 6 and 10 and time_of_day != 'Morning')
    or (started_hour between 11 and 15 and time_of_day != 'Afternoon')
    or (started_hour between 16 and 18 and time_of_day != 'Evening')
    or (started_hour between 19 and 23 and time_of_day != 'Night')