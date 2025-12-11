{{
    config(
        materialized='table',
        order_by=['ride_hour', 'is_weekend']
    )
}}

SELECT
    ride_hour,
    is_weekend,
    
    -- Ride counts
    count(*) AS total_rides,
    countIf(member_casual = 'member') AS member_rides,
    countIf(member_casual = 'casual') AS casual_rides,
    
    -- Duration and distance
    round(avg(ride_duration_minutes), 2) AS avg_ride_duration,
    round(avg(distance_km), 2) AS avg_distance_km,
    
    -- Unique stations
    uniq(start_station_id) AS unique_start_stations,
    uniq(end_station_id) AS unique_end_stations

FROM {{ ref('fct_rides') }}
GROUP BY 
    ride_hour,
    is_weekend
ORDER BY 
    is_weekend,
    ride_hour