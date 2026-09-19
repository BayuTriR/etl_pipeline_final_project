select
    ride_id,
    trip_duration_seconds,
    trip_duration_minutes,
    round(trip_duration_seconds / 60.0, 2) as expected_minutes
from {{ ref('int_citibike_trips') }}
where abs(trip_duration_minutes - round(trip_duration_seconds / 60.0, 2)) > 0.01