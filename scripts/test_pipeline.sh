#!/bin/bash

set -e

echo "==================================="
echo "Testing Bike Share Analytics Pipeline"
echo "==================================="

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Step 1: Check if .env file exists
echo -e "\n${YELLOW}[1/6] Checking environment file...${NC}"
if [ ! -f .env ]; then
    echo -e "${RED}Error: .env file not found${NC}"
    echo "Creating .env from .env.example..."
    cp .env.example .env
    echo -e "${GREEN}✓ Created .env file${NC}"
else
    echo -e "${GREEN}✓ .env file exists${NC}"
fi

# Load environment variables
export $(grep -v '^#' .env | xargs)

# Step 2: Start Docker services
echo -e "\n${YELLOW}[2/6] Starting Docker services...${NC}"
docker compose up -d
echo -e "${GREEN}✓ Services started${NC}"

# Wait for services to be healthy
echo -e "\n${YELLOW}[3/6] Waiting for services to be ready...${NC}"
sleep 10

# Check ClickHouse health
echo "Checking ClickHouse..."
for i in {1..60}; do
    if docker compose exec -T clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ ClickHouse is ready${NC}"
        break
    fi
    if [ $i -eq 60 ]; then
        echo -e "${RED}Error: ClickHouse failed to start${NC}"
        docker compose logs clickhouse
        exit 1
    fi
    [ $((i % 10)) -eq 0 ] && echo "  Still waiting... ($i/60)"
    sleep 1
done

# Verify ClickHouse database exists
echo "Verifying ClickHouse database..."
docker compose exec -T clickhouse clickhouse-client --query "SHOW DATABASES" | grep -q bikeshare && \
    echo -e "${GREEN}✓ Database 'bikeshare' exists${NC}" || \
    echo -e "${YELLOW}⚠ Database 'bikeshare' not found, but continuing...${NC}"

# Step 4: Download and load test data
echo -e "\n${YELLOW}[4/6] Downloading and loading test data (2 files)...${NC}"
docker compose exec -T dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --download \
    --limit 2 \
    --dir /usr/app/data/raw

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Data downloaded and loaded${NC}"
else
    echo -e "${RED}Error: Failed to download/load data${NC}"
    docker compose logs dbt-dev
    exit 1
fi

# Step 5: Verify data in ClickHouse
echo -e "\n${YELLOW}[5/6] Verifying data in ClickHouse...${NC}"
docker compose exec -T clickhouse clickhouse-client --query "
SELECT 
    count(*) as total_rides,
    min(started_at) as earliest_ride,
    max(started_at) as latest_ride,
    countDistinct(member_casual) as user_types,
    countDistinct(rideable_type) as bike_types
FROM bikeshare.raw_rides
FORMAT Pretty
"

# Step 6: Run dbt models
echo -e "\n${YELLOW}[6/6] Running dbt models...${NC}"

# Install dbt packages
echo "Installing dbt packages..."
docker compose exec -T dbt-dev bash -c "cd /usr/app/dbt_project && dbt deps"

# Debug dbt connection
echo "Testing dbt connection..."
docker compose exec -T dbt-dev bash -c "cd /usr/app/dbt_project && dbt debug"

# Run dbt models
echo "Running dbt transformations..."
docker compose exec -T dbt-dev bash -c "cd /usr/app/dbt_project && dbt run"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ dbt models executed successfully${NC}"
else
    echo -e "${RED}Error: dbt run failed${NC}"
    docker compose logs dbt-dev
    exit 1
fi

# Verify dbt models
echo -e "\n${YELLOW}Verifying dbt models...${NC}"
docker compose exec -T clickhouse clickhouse-client --query "
SELECT 
    'fct_rides' as table_name,
    count(*) as row_count
FROM bikeshare_marts.fct_rides
UNION ALL
SELECT 
    'dim_stations',
    count(*)
FROM bikeshare_marts.dim_stations
UNION ALL
SELECT 
    'agg_daily_rides',
    count(*)
FROM bikeshare_marts.agg_daily_rides
UNION ALL
SELECT 
    'agg_hourly_patterns',
    count(*)
FROM bikeshare_marts.agg_hourly_patterns
FORMAT Pretty
"

echo -e "\n${GREEN}==================================="
echo "✓ Pipeline test completed successfully!"
echo "===================================${NC}"

echo -e "\n${YELLOW}Data Summary:${NC}"
docker compose exec -T clickhouse clickhouse-client --query "
SELECT 
    'Raw Rides' as metric,
    formatReadableQuantity(count(*)) as value
FROM bikeshare.raw_rides
UNION ALL
SELECT 
    'Transformed Rides',
    formatReadableQuantity(count(*))
FROM bikeshare_marts.fct_rides
UNION ALL
SELECT 
    'Unique Stations',
    formatReadableQuantity(count(*))
FROM bikeshare_marts.dim_stations
FORMAT Pretty
"

echo -e "\n${YELLOW}Next steps:${NC}"
echo "  1. View logs: docker compose logs -f dbt-dev"
echo "  2. Access ClickHouse CLI: docker compose exec clickhouse clickhouse-client"
echo "  3. Run dbt tests: docker compose exec dbt-dev bash -c 'cd /usr/app/dbt_project && dbt test'"
echo "  4. Generate docs: docker compose exec dbt-dev bash -c 'cd /usr/app/dbt_project && dbt docs generate'"
echo "  5. Stop services: docker compose down"
echo "  6. Clean data: docker compose down -v"