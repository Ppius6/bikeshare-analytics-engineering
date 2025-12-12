# Bike Share Analytics Engineering Documentation

## Project Overview

This project implements a data analytics pipeline for Citi Bike Jersey City trip data (2025). It uses a modern data stack containerized with Docker, allowing for flexible development against both local resources and cloud infrastructure.

* **Data Source**: Citi Bike Jersey City trip data (S3)
* **Database**: ClickHouse (OLAP) - Supports both **Local Docker** and **ClickHouse Cloud**
* **Transformation**: dbt (data build tool)
* **Orchestration**: Python scripts & Docker Compose

## Architecture

![archictecture](image.png)

## Data Flow

1. **Extract**: Python script downloads CSV files from an S3 bucket or reads from a local directory.
2. **Load**: Python script ingests raw data into ClickHouse.
      * *Option A:* Loads into local ClickHouse container.
      * *Option B:* Loads into ClickHouse Cloud via secure HTTP.
3. **Transform**: dbt models process data through `staging` -\> `intermediate` -\> `marts`.
4. **Test**: dbt generic and singular tests validate data quality and integrity.

## Quick Start

See [Getting Started Guide](https://www.google.com/search?q=getting-started.md)

## Running the Project

The project is configured to switch easily between local development and cloud production using the `--target` flag.

### 1\. Start Environment

```bash
docker-compose up -d
docker-compose exec dbt-dev bash
```

### 2\. Choose Your Workflow
Here are the corrected sections with options for both **Sample Data** (fast) and **Full Data** (production load).

#### Option A: Local Development (Default)

Ideal for rapid testing, debugging, and offline work. Data stays inside the Docker network.

```bash
# --- Option 1: Quick Start (Sample Data) ---
# Download and load only 2 files from S3
python /usr/app/scripts/load_to_clickhouse.py --download --limit 2

# --- Option 2: Full Pipeline (All Data) ---
# Load all CSV files currently in your local data directory
python /usr/app/scripts/load_to_clickhouse.py --dir /usr/app/data/raw

# Run transformations locally
dbt run

# Test local data
dbt test

# or combine altogether
dbt build
```

#### Option B: ClickHouse Cloud (Production-Ready)

Pushes data to your managed ClickHouse Cloud instance. Requires `CLICKHOUSE_CLOUD_*` variables in `.env`.

```bash
# --- Option 1: Cloud Test (Sample Data) ---
# Download and load only 2 files to Cloud
python /usr/app/scripts/load_to_clickhouse.py --download --limit 2 --target cloud

# --- Option 2: Production Load (Full Data) ---
# Load all local CSV files to Cloud
python /usr/app/scripts/load_to_clickhouse.py --dir /usr/app/data/raw --target cloud

# Run transformations against Cloud
dbt run --target cloud

# Test Cloud data
dbt test --target cloud

# Or combine altogether
dbt build --target cloud
```

## Configuration (.env)

Ensure your `.env` file contains configuration for both targets:

```dotenv
# Local Connection (Internal Docker Network)
CLICKHOUSE_HOST=clickhouse
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_DB=bikeshare

# Cloud Connection (External)
CLICKHOUSE_CLOUD_HOST=your-id.region.aws.clickhouse.cloud
CLICKHOUSE_CLOUD_HTTP_PORT=8443
CLICKHOUSE_CLOUD_NATIVE_PORT=9440
CLICKHOUSE_CLOUD_PASSWORD=your_password
CLICKHOUSE_CLOUD_USER=default
```

## Directory Structure

```
.
├── data/                    # Local data storage (ignored by git)
├── data_ingestion/          # Python ETL code
│   ├── extractors/          # S3 downloader logic
│   └── utils/               # Data cleaning & validation
├── dbt_project/             # dbt transformations
│   ├── models/
│   │   ├── staging/         # View materializations (renaming/casting)
│   │   ├── intermediate/    # Ephemeral models (complex logic)
│   │   └── marts/           # Table materializations (business BI layers)
│   ├── tests/               # Data quality tests
│   └── profiles.yml         # Connection profiles (dev/cloud)
├── docker/                  # Docker configurations
├── scripts/                 # Entry point scripts (load_to_clickhouse.py)
└── docs/                    # Documentation
```
