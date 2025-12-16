CREATE DATABASE IF NOT EXISTS bikeshare;

CREATE TABLE IF NOT EXISTS bikeshare.raw_rides (
    ride_id String CODEC(ZSTD(1)),
    rideable_type LowCardinality(String),
    started_at DateTime64(3),
    ended_at DateTime64(3),
    start_station_name String CODEC(ZSTD(1)),
    start_station_id LowCardinality(String),
    end_station_name String CODEC(ZSTD(1)),
    end_station_id LowCardinality(String),
    start_lat Float64 DEFAULT 0,
    start_lng Float64 DEFAULT 0,
    end_lat Float64 DEFAULT 0,
    end_lng Float64 DEFAULT 0,
    member_casual LowCardinality(String),
    source_file String DEFAULT '' CODEC(ZSTD(1)),
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (toDate(started_at), member_casual, rideable_type, ride_id)
PARTITION BY toYYYYMM(started_at);

-- Create index for faster duplicate checks
CREATE INDEX IF NOT EXISTS idx_source_file ON bikeshare.raw_rides (source_file) TYPE minmax GRANULARITY 1;

ALTER TABLE bikeshare.raw_rides ADD INDEX idx_member_casual member_casual TYPE set(0) GRANULARITY 4;
ALTER TABLE bikeshare.raw_rides ADD INDEX idx_rideable_type rideable_type TYPE set(0) GRANULARITY 4;

-- Add projections for common query patterns (no ORDER BY for aggregating projections)
ALTER TABLE bikeshare.raw_rides
ADD PROJECTION IF NOT EXISTS proj_member_analysis (
    SELECT
        member_casual,
        rideable_type,
        toDate(started_at) AS ride_date,
        count() AS total_rides,
        avg(dateDiff('minute', started_at, ended_at)) AS avg_duration
    GROUP BY member_casual, rideable_type, ride_date
);

ALTER TABLE bikeshare.raw_rides
ADD PROJECTION IF NOT EXISTS proj_hourly_station (
    SELECT
        start_station_id,
        toHour(started_at) AS hour,
        toDayOfWeek(started_at) IN (6, 7) AS is_weekend,
        count() AS ride_count
    GROUP BY start_station_id, hour, is_weekend
);