{{
    config(
        materialized='view'
    )
}}

WITH start_stations AS (
    SELECT DISTINCT
        start_station_id AS station_id,
        start_station_name AS station_name,
        start_lat AS latitude,
        start_lng AS longitude
    FROM {{ source('bikeshare', 'raw_rides') }}
    WHERE start_station_id IS NOT NULL
      AND start_station_name IS NOT NULL
),

end_stations AS (
    SELECT DISTINCT
        end_station_id AS station_id,
        end_station_name AS station_name,
        end_lat AS latitude,
        end_lng AS longitude
    FROM {{ source('bikeshare', 'raw_rides') }}
    WHERE end_station_id IS NOT NULL
      AND end_station_name IS NOT NULL
),

all_stations AS (
    SELECT * FROM start_stations
    UNION ALL
    SELECT * FROM end_stations
)

SELECT
    station_id,
    any(station_name) AS station_name,
    avg(latitude) AS latitude,
    avg(longitude) AS longitude
FROM all_stations
GROUP BY station_id