{{
    config(
        materialized='table',
        order_by=['ride_date', 'member_casual', 'rideable_type', 'started_at']
    )
}}

SELECT
    ride_id,
    rideable_type,
    started_at,
    ended_at,
    ride_duration_minutes,
    distance_km,
    avg_speed_kmh,
    start_station_id,
    end_station_id,
    member_casual,
    ride_date,
    ride_hour,
    day_of_week,
    ride_month,
    ride_year,
    time_of_day,
    is_weekend,
    
    -- Business metrics
    CASE 
        WHEN ride_duration_minutes <= 30 THEN 'Short (0-30 min)'
        WHEN ride_duration_minutes <= 60 THEN 'Medium (30-60 min)'
        WHEN ride_duration_minutes <= 120 THEN 'Long (1-2 hours)'
        ELSE 'Very Long (2+ hours)'
    END AS ride_length_category,
    
    CASE 
        WHEN distance_km IS NULL THEN 'Unknown'
        WHEN distance_km <= 2 THEN 'Short (0-2 km)'
        WHEN distance_km <= 5 THEN 'Medium (2-5 km)'
        WHEN distance_km <= 10 THEN 'Long (5-10 km)'
        ELSE 'Very Long (10+ km)'
    END AS distance_category

FROM {{ ref('int_ride_metrics') }}