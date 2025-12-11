# Bike Share Analytics Engineering Documentation

## Project Overview

This project implements a data analytics pipeline for bike share data using:
- **Data Source**: Citi Bike Jersey City trip data from S3 focusing on 2025
- **Database**: ClickHouse (OLAP for fast analytics)
- **Transformation**: dbt (data build tool)

while containerized in a docker container.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│   S3 Bucket │────▶│ Python ETL   │────▶│ ClickHouse  │────▶│ dbt Models   │
│  (Raw Data) │     │  (Ingestion) │     │  (Raw Data) │     │ (Analytics)  │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────────┘
```

## Data Flow

1. **Extract**: Download CSV files from S3
2. **Load**: Insert raw data into ClickHouse
3. **Transform**: dbt models create staging, facts, and aggregations
4. **Test**: dbt tests validate data quality

## Quick Start

See [Getting Started Guide](getting-started.md)

## Directory Structure

```
.
├── data/                     # Local data storage
├── data_ingestion/          # Python ETL code
│   ├── extractors/          # S3 downloader
│   └── utils/               # Data cleaning utilities
├── dbt_project/             # dbt transformations
│   ├── models/
│   │   ├── staging/         # Source data cleanup
│   │   └── marts/           # Business logic layer
│   └── tests/               # Data quality tests
├── docker/                  # Docker configurations
├── scripts/                 # Utility scripts
└── docs/                    # This documentation
```