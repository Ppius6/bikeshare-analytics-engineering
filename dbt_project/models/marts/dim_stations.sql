{{
    config(
        materialized='table',
        engine='MergeTree()',
        order_by=['station_id']
    )
}}

WITH combined_stats AS (
    SELECT
        start_station_id AS station_id,
        'start' AS direction,
        member_casual,
        ride_duration_minutes,
        ride_date
    FROM {{ ref('stg_rides') }}
    WHERE start_station_id IS NOT NULL
    
    UNION ALL
    
    SELECT
        end_station_id AS station_id,
        'end' AS direction,
        NULL AS member_casual,
        NULL AS ride_duration_minutes,
        NULL AS ride_date
    FROM {{ ref('stg_rides') }}
    WHERE end_station_id IS NOT NULL
),

station_aggregates AS (
    SELECT
        station_id,
        countIf(direction = 'start') AS total_starts,
        countIf(direction = 'end') AS total_ends,
        countIf(direction = 'start' AND member_casual = 'member') AS member_starts,
        countIf(direction = 'start' AND member_casual = 'casual') AS casual_starts,
        avgIf(ride_duration_minutes, direction = 'start') AS avg_ride_duration,
        minIf(ride_date, direction = 'start') AS first_ride_date,
        maxIf(ride_date, direction = 'start') AS last_ride_date
    FROM combined_stats
    GROUP BY station_id
)

SELECT
    s.station_id AS station_id,
    s.station_name AS station_name,
    s.latitude AS latitude,
    s.longitude AS longitude,
    coalesce(sa.total_starts, 0) AS total_rides_started,
    coalesce(sa.total_ends, 0) AS total_rides_ended,
    coalesce(sa.total_starts, 0) + coalesce(sa.total_ends, 0) AS total_rides,
    coalesce(sa.member_starts, 0) AS member_rides_started,
    coalesce(sa.casual_starts, 0) AS casual_rides_started,
    round(sa.avg_ride_duration, 2) AS avg_ride_duration_minutes,
    sa.first_ride_date,
    sa.last_ride_date,
    dateDiff('day', sa.first_ride_date, sa.last_ride_date) + 1 AS days_active
FROM {{ ref('stg_stations') }} s
LEFT JOIN station_aggregates sa ON s.station_id = sa.station_id