{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    ride_duration_minutes,
    start_station_id,
    end_station_id,
    member_casual,
    ride_date,
    ride_hour,
    day_of_week,
    ride_month,
    ride_year,
    start_lat != 0 AND start_lng != 0 AND end_lat != 0 AND end_lng != 0 AS has_valid_coords,
    -- Distance calculation (Haversine formula approximation)
    CASE 
        WHEN start_lat != 0 AND start_lng != 0 
         AND end_lat != 0 AND end_lng != 0
        THEN round(
            geoDistance(start_lng, start_lat, end_lng, end_lat) / 1000, 2
        )
        ELSE NULL
    END AS distance_km, 
    
    -- Time-based features
    CASE 
        WHEN ride_hour BETWEEN 6 AND 9 THEN 'Morning Rush'
        WHEN ride_hour BETWEEN 10 AND 15 THEN 'Midday'
        WHEN ride_hour BETWEEN 16 AND 19 THEN 'Evening Rush'
        WHEN ride_hour BETWEEN 20 AND 23 THEN 'Evening'
        ELSE 'Night'
    END AS time_of_day,
    
    CASE 
        WHEN day_of_week IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS is_weekend,
    
    -- Speed calculation (km/h)
    CASE 
        WHEN ride_duration_minutes > 0 
         AND start_lat != 0 AND start_lng != 0 
        THEN round(
            (geoDistance(start_lng, start_lat, end_lng, end_lat) / 1000) / 
            (ride_duration_minutes / 60), 2
        )
        ELSE NULL
    END AS avg_speed_kmh

FROM {{ ref('stg_rides') }}
WHERE ride_duration_minutes > 0