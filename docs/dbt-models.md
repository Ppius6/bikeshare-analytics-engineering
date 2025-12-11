# dbt Models Documentation

## Model Lineage

```
raw_rides (source)
    │
    ├─▶ stg_rides ────┬─▶ fct_rides ────┬─▶ agg_daily_rides
    │                 │                 │
    └─▶ stg_stations ─┴─▶ dim_stations  └─▶ agg_hourly_patterns
```

## Model Details

### Staging Models

#### `stg_rides`
**Purpose**: Clean and enrich raw ride data

**Transformations**:
- Calculate ride duration in minutes
- Extract ride date
- Calculate distance using haversine formula
- Convert timestamps to proper format

**Output columns**: 13 fields
**Materialization**: View (no data storage)

#### `stg_stations`
**Purpose**: Create unique station list

**Transformations**:
- Deduplicate stations (same ID, different names)
- Use `any()` for name selection
- Average coordinates for same station

**Output columns**: 4 fields
**Materialization**: View

### Marts Models

#### `fct_rides`
**Purpose**: Analytics-ready ride facts

**Business rules**: None (passes through from staging)
**Grain**: One row per ride
**Materialization**: Table (MergeTree)
**Partitioning**: By month (toYYYYMM(ride_date))

#### `dim_stations`
**Purpose**: Station master data with metrics

**Metrics calculated**:
- Total rides (started + ended)
- Member vs casual breakdown
- Average ride duration
- Date range active
- Days active

**Grain**: One row per station
**Materialization**: Table (MergeTree)

#### `agg_daily_rides`
**Purpose**: Daily summary metrics

**Metrics**:
- Total rides per day
- Member/casual breakdown
- Average duration
- Average distance
- Unique stations used

**Grain**: One row per date
**Materialization**: Table (MergeTree)

#### `agg_hourly_patterns`
**Purpose**: Usage patterns by time

**Dimensions**: Hour, Weekend/Weekday
**Metrics**:
- Total rides
- Member/casual breakdown
- Average duration/distance
- Unique stations

**Grain**: One row per (hour, is_weekend)
**Materialization**: Table (MergeTree)

## Performance Optimizations

### ClickHouse Indexes
- Primary key ordering for time-based queries
- Set indexes on `member_casual`, `rideable_type`
- Minmax index on `source_file`

### Partitioning
- Monthly partitions on fact table
- Enables efficient data pruning
- Faster queries with date filters