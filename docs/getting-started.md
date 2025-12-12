# Getting Started

## Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local development)
- 4GB+ RAM available
- *Optional:* ClickHouse Cloud credentials (if deploying to production)

## Setup

### 1\. Clone and Configure

```bash
git clone <repo-url>
cd bikeshare-analytics-engineering

# Create environment file
cp .env.example .env

# Note: If using Cloud, ensure CLICKHOUSE_CLOUD_* variables are set in .env
```

### 2\. Start Services

```bash
# Start all services
docker compose up -d

# Check health
docker compose ps
```

### 3\. Load Data

You can load data into your **Local** Docker container or push it directly to **ClickHouse Cloud**.

**Option A: Load to Local (Default)**

```bash
# Load just 2 files for testing
docker compose exec dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --download \
    --limit 2 

# OR Load all data
docker compose exec dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --dir /usr/app/data/raw
```

**Option B: Load to Cloud**

```bash
# Load just 2 files to Cloud
docker compose exec dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --download \
    --limit 2 \
    --target cloud

# OR Load all data to Cloud
docker compose exec dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --dir /usr/app/data/raw \
    --target cloud
```

### 4\. Run Transformations

**Option A: Local Development**

```bash
# Build and Test models locally
docker compose exec dbt-dev dbt build
```

**Option B: ClickHouse Cloud**

```bash
# Build and Test models in the Cloud
docker compose exec dbt-dev dbt build --target cloud
```

*(Note: `dbt build` runs both models and tests in dependency order. Use `--full-refresh` if you need to completely rebuild tables.)*

### 5\. Query Data (Local Only)

To query your **Local** database via the command line:

```bash
# Access ClickHouse client
docker compose exec clickhouse clickhouse-client

# Or run queries directly
docker compose exec clickhouse clickhouse-client --query "
SELECT count(*) FROM bikeshare_marts.fct_rides
"
```

*For **Cloud** queries, use the ClickHouse Cloud web console.*

## Common Commands

```bash
# Stop services
docker compose down

# Clean everything (including local data volumes)
docker compose down -v

# Rebuild container after code changes
docker compose up -d --build dbt-dev

# View logs
docker compose logs -f dbt-dev
docker compose logs -f clickhouse
```
