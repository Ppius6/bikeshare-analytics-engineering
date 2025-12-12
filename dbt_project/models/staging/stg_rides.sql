{{config(materialized='view'
    )
}}

SELECT
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    dateDiff('minute', started_at, ended_at) AS ride_duration_minutes,
    start_station_name,
    start_station_id,
    end_station_name,
    end_station_id,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    toDate(started_at) AS ride_date,
    toHour(started_at) AS ride_hour,
    toDayOfWeek(started_at) AS day_of_week,
    toMonth(started_at) AS ride_month,
    toYear(started_at) AS ride_year,
    loaded_at
FROM {{ source('bikeshare', 'raw_rides') }}
WHERE started_at < ended_at
  AND dateDiff('minute', started_at, ended_at) >= 1
  AND dateDiff('minute', started_at, ended_at) <= 1440
ORDER BY loaded_at DESC
LIMIT 1 BY ride_id