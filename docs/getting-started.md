# Getting Started

## Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local development)
- 4GB+ RAM available

## Setup

### 1. Clone and Configure

```bash
git clone <repo-url>
cd bikeshare-analytics-engineering

# Create environment file
cp .env.example .env
```

### 2. Start Services

```bash
# Start all services
docker compose up -d

# Check health
docker compose ps
```

### 3. Load Data

```bash
# Load all data from S3
docker compose exec dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --download \
    --dir /usr/app/data/raw

# Or load just 2 files for testing
docker compose exec dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --download \
    --limit 2 \
    --dir /usr/app/data/raw
```

### 4. Run Transformations

```bash
# Run all dbt models
docker compose exec dbt-dev dbt run

# Run tests
docker compose exec dbt-dev dbt test
```

### 5. Query Data

```bash
# Access ClickHouse client
docker compose exec clickhouse clickhouse-client

# Or run queries directly
docker compose exec clickhouse clickhouse-client --query "
SELECT count(*) FROM bikeshare_marts.fct_rides
"
```

## Common Commands

```bash
# Stop services
docker compose down

# Clean everything (including data)
docker compose down -v

# Rebuild after code changes
docker compose build dbt-dev

# View logs
docker compose logs -f dbt-dev
docker compose logs -f clickhouse
```