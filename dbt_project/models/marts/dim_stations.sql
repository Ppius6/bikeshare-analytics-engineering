{{
    config(
        materialized='table',
        engine='MergeTree()'
    )
}}

WITH station_stats AS (
    SELECT
        start_station_id AS station_id,
        count(*) AS total_starts,
        countIf(member_casual = 'member') AS member_starts,
        countIf(member_casual = 'casual') AS casual_starts,
        avg(ride_duration_minutes) AS avg_ride_duration,
        min(ride_date) AS first_ride_date,
        max(ride_date) AS last_ride_date
    FROM {{ ref('stg_rides') }}
    WHERE start_station_id IS NOT NULL
    GROUP BY start_station_id
),

end_stats AS (
    SELECT
        end_station_id AS station_id,
        count(*) AS total_ends
    FROM {{ ref('stg_rides') }}
    WHERE end_station_id IS NOT NULL
    GROUP BY end_station_id
)

SELECT
    s.station_id AS station_id,
    s.station_name AS station_name,
    s.latitude AS latitude,
    s.longitude AS longitude,
    coalesce(ss.total_starts, 0) AS total_rides_started,
    coalesce(es.total_ends, 0) AS total_rides_ended,
    coalesce(ss.total_starts, 0) + coalesce(es.total_ends, 0) AS total_rides,
    coalesce(ss.member_starts, 0) AS member_rides_started,
    coalesce(ss.casual_starts, 0) AS casual_rides_started,
    round(ss.avg_ride_duration, 2) AS avg_ride_duration_minutes,
    ss.first_ride_date,
    ss.last_ride_date,
    dateDiff('day', ss.first_ride_date, ss.last_ride_date) + 1 AS days_active
FROM {{ ref('stg_stations') }} s
LEFT JOIN station_stats ss ON s.station_id = ss.station_id
LEFT JOIN end_stats es ON s.station_id = es.station_id