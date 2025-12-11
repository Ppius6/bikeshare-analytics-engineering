# Data Pipeline Documentation

## Data Ingestion

### Source Data
- **Provider**: Citi Bike Jersey City
- **Format**: CSV files (zipped)
- **Location**: S3 bucket
- **Frequency**: Monthly releases
- **Coverage**: January 2025 - November 2025 (11 files, ~950K rides)

### ETL Process

#### 1. Extraction (`data_ingestion/extractors/s3_downloader.py`)
- Scrapes S3 bucket for available files
- Downloads ZIP archives
- Extracts CSV files
- Supports parallel downloads

#### 2. Cleaning (`data_ingestion/utils/data_cleaner.py`)
- Converts data types
- Handles missing values
- Validates required fields
- Filters invalid rides (< 60s or > 24h)

#### 3. Loading (`scripts/load_to_clickhouse.py`)
- Batch inserts (10,000 rows per batch)
- Source file tracking
- Deduplication (skips already-loaded files)
- Retry logic (3 attempts, 5s delay)
- Progress monitoring with tqdm

### Data Quality Rules

**Dropped Records:**
- Missing required fields (ride_id, started_at, ended_at, member_casual)
- Invalid datetime ranges (end_at <= started_at)
- Duration < 60 seconds (test rides)
- Duration > 24 hours (likely errors)

**Field Transformations:**
- NaN values in string fields → empty strings
- Invalid coordinates → NULL
- Datetime parsing with error handling

## dbt Models

### Staging Layer (`models/staging/`)

#### `stg_rides.sql`
- Source: `bikeshare.raw_rides`
- Adds calculated fields:
  - `ride_duration_minutes`
  - `ride_date`
  - `distance_km` (haversine formula)

#### `stg_stations.sql`
- Deduplicates stations from ride data
- Uses `any()` to handle station name variations
- Averages coordinates for same station_id

### Marts Layer (`models/marts/`)

#### `fct_rides.sql` (Fact Table)
- Grain: One row per ride
- ~950K records
- Partitioned by month

#### `dim_stations.sql` (Dimension Table)
- Grain: One row per station
- ~470 unique stations
- Station-level metrics

#### `agg_daily_rides.sql` (Aggregate)
- Grain: One row per date
- Daily ride metrics

#### `agg_hourly_patterns.sql` (Aggregate)
- Grain: One row per (hour, is_weekend)
- Usage patterns analysis

## Data Quality Tests

All tests defined in `models/*/schema.yml`:

**Uniqueness:**
- `ride_id` in fact table
- `station_id` in dimension table
- `ride_date` in daily aggregates

**Not Null:**
- All primary keys
- Required fields (started_at, ended_at, etc.)

**Accepted Values:**
- `member_casual` in ['member', 'casual']

**Source Tests:**
- Validates raw data before transformation