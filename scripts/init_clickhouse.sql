CREATE DATABASE IF NOT EXISTS bikeshare;

CREATE TABLE IF NOT EXISTS bikeshare.raw_rides (
    ride_id String,
    rideable_type LowCardinality(String),
    started_at DateTime64(3),
    ended_at DateTime64(3),
    start_station_name String,
    start_station_id String,
    end_station_name String,
    end_station_id String,
    start_lat Nullable(Float64),
    start_lng Nullable(Float64),
    end_lat Nullable(Float64),
    end_lng Nullable(Float64),
    member_casual LowCardinality(String),
    source_file String DEFAULT '',
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (started_at, ride_id)
PARTITION BY toYYYYMM(started_at);

-- Create index for faster duplicate checks
CREATE INDEX IF NOT EXISTS idx_source_file ON bikeshare.raw_rides (source_file) TYPE minmax GRANULARITY 1;

ALTER TABLE bikeshare.raw_rides ADD INDEX idx_member_casual member_casual TYPE set(0) GRANULARITY 4;
ALTER TABLE bikeshare.raw_rides ADD INDEX idx_rideable_type rideable_type TYPE set(0) GRANULARITY 4;