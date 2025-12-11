set -e

echo "🚴 Starting Bike Share Analytics Pipeline..."

echo "📥 Loading data from S3..."
docker compose exec -T dbt-dev python /usr/app/scripts/load_to_clickhouse.py \
    --download --dir /usr/app/data/raw

echo "🔄 Running dbt transformations..."
docker compose exec -T dbt-dev dbt run

echo "✅ Running data quality tests..."
docker compose exec -T dbt-dev dbt test

echo "✨ Pipeline complete!"