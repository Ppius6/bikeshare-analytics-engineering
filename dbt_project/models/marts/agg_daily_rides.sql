{{
    config(
        materialized='incremental',
        unique_key='ride_date',
        order_by=['ride_date'],
        incremental_strategy='delete+insert'
    )
}}

SELECT
    ride_date,
    day_of_week,
    is_weekend,
    ride_month,
    ride_year,
    
    -- Ride counts
    count(*) AS total_rides,
    countIf(member_casual = 'member') AS member_rides,
    countIf(member_casual = 'casual') AS casual_rides,
    countIf(rideable_type = 'electric_bike') AS electric_bike_rides,
    countIf(rideable_type = 'classic_bike') AS classic_bike_rides,
    
    -- Duration metrics
    round(avg(ride_duration_minutes), 2) AS avg_ride_duration,
    round(median(ride_duration_minutes), 2) AS median_ride_duration,
    min(ride_duration_minutes) AS min_ride_duration,
    max(ride_duration_minutes) AS max_ride_duration,
    
    -- Distance metrics
    round(avg(distance_km), 2) AS avg_distance_km,
    round(sum(distance_km), 2) AS total_distance_km,
    
    -- Speed metrics
    round(avg(avg_speed_kmh), 2) AS avg_speed_kmh,
    
    -- Time of day distribution
    countIf(time_of_day = 'Morning Rush') AS morning_rush_rides,
    countIf(time_of_day = 'Midday') AS midday_rides,
    countIf(time_of_day = 'Evening Rush') AS evening_rush_rides,
    countIf(time_of_day = 'Evening') AS evening_rides,
    countIf(time_of_day = 'Night') AS night_rides

FROM {{ ref('fct_rides') }}

{% if is_incremental() %} 
WHERE ride_date NOT IN (SELECT max(ride_date) FROM {{ this }})
{% endif %}

GROUP BY 
    ride_date,
    day_of_week,
    is_weekend,
    ride_month,
    ride_year
ORDER BY ride_date DESC