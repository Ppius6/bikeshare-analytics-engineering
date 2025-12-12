#!/usr/bin/env python3
"""
Load bike share CSV data into ClickHouse.
Usage: python load_to_clickhouse.py --file data.csv --target cloud
       python load_to_clickhouse.py --dir ./data/raw --target dev
       python load_to_clickhouse.py --download --limit 5 --target cloud
"""

import argparse
import logging
import os
import sys
import time
from functools import wraps
from pathlib import Path
from typing import Optional

import clickhouse_connect
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_ingestion.extractors.s3_downloader import S3BikeShareDownloader
from data_ingestion.utils.data_cleaner import BikeShareCleaner

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def retry(max_attempts=3, delay=5):
    """Retry decorator for transient failures."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts:
                        logger.error(f"❌ Failed after {max_attempts} attempts: {e}")
                        raise
                    logger.warning(
                        f"⚠️  Attempt {attempt}/{max_attempts} failed: {e}. "
                        f"Retrying in {delay}s..."
                    )
                    time.sleep(delay)

        return wrapper

    return decorator


def check_file_already_loaded(client, filename):
    """Check if file was already loaded."""
    try:
        # Create table if not exists to avoid error on first run
        client.command(
            """
            CREATE TABLE IF NOT EXISTS raw_rides (
                ride_id String,
                rideable_type String,
                started_at DateTime,
                ended_at DateTime,
                start_station_name String,
                start_station_id String,
                end_station_name String,
                end_station_id String,
                start_lat Float64,
                start_lng Float64,
                end_lat Float64,
                end_lng Float64,
                member_casual String,
                source_file String,
                loaded_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY (started_at, ride_id)
        """
        )

        result = client.query(
            f"SELECT count(*) FROM raw_rides WHERE source_file = '{filename}'"
        ).result_rows
        return result[0][0] > 0
    except Exception as e:
        logger.warning(f"Could not check if file loaded (table might be empty): {e}")
        return False


@retry(max_attempts=3, delay=5)
def get_clickhouse_client(target: str = "dev"):
    """
    Create ClickHouse client connection based on target.
    target: 'dev' (Local) or 'cloud' (ClickHouse Cloud)
    """
    try:
        if target == "cloud":
            host = os.getenv("CLICKHOUSE_CLOUD_HOST")
            # Prefer HTTP port for Python client, fallback to standard 8443
            port = int(os.getenv("CLICKHOUSE_CLOUD_HTTP_PORT", "8443"))
            user = os.getenv("CLICKHOUSE_CLOUD_USER", "default")
            password = os.getenv("CLICKHOUSE_CLOUD_PASSWORD")
            database = os.getenv("CLICKHOUSE_CLOUD_DB", "default")
            secure = True
            label = "ClickHouse Cloud ☁️"
        else:
            host = os.getenv("CLICKHOUSE_HOST", "localhost")
            # Check for specific HTTP port first, then generic PORT
            port = int(
                os.getenv("CLICKHOUSE_HTTP_PORT", os.getenv("CLICKHOUSE_PORT", "8123"))
            )
            user = os.getenv("CLICKHOUSE_USER", "default")
            password = os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
            database = os.getenv("CLICKHOUSE_DB", "bikeshare")
            secure = os.getenv("CLICKHOUSE_SECURE", "false").lower() == "true"
            label = "ClickHouse Local 🏠"

        if not host:
            raise ValueError(
                f"Host not found for target '{target}'. Check .env variables."
            )

        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            secure=secure,
            send_receive_timeout=300,  # Extended timeout for cloud operations
        )

        masked_host = host[:15] + "..." if len(host) > 15 else host
        logger.info(f"Successfully connected to {label} at {masked_host}:{port}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse ({target}): {e}")
        raise


def validate_loaded_data(client):
    """Generate data quality report after loading."""
    logger.info("\n" + "=" * 60)
    logger.info("DATA VALIDATION REPORT")
    logger.info("=" * 60)

    try:
        # Total records
        total = client.query("SELECT count(*) FROM raw_rides").result_rows[0][0]
        logger.info(f"📊 Total records: {total:,}")

        if total == 0:
            logger.info("⚠️  Table is empty.")
            return

        # Date range
        date_range = client.query(
            """
            SELECT min(started_at), max(started_at) 
            FROM raw_rides
        """
        ).result_rows[0]
        logger.info(f"📅 Date range: {date_range[0]} to {date_range[1]}")

        # Null checks
        nulls = client.query(
            """
            SELECT 
                countIf(ride_id = '') as empty_ride_id,
                countIf(started_at = toDateTime('1970-01-01 00:00:00')) as empty_started,
                countIf(start_station_id = '') as empty_start_station,
                countIf(end_station_id = '') as empty_end_station
            FROM raw_rides
        """
        ).result_rows[0]

        logger.info(f"\n🔍 Data Quality Checks (Empty/Default Values):")
        logger.info(f"  Empty ride_id: {nulls[0]:,}")
        logger.info(f"  Default/Null started_at: {nulls[1]:,}")
        logger.info(f"  Empty start_station: {nulls[2]:,}")
        logger.info(f"  Empty end_station: {nulls[3]:,}")

        # Files loaded
        files = client.query(
            """
            SELECT source_file, count(*) as cnt, min(loaded_at) as first_load
            FROM raw_rides
            WHERE source_file != ''
            GROUP BY source_file
            ORDER BY source_file
        """
        ).result_rows

        logger.info(f"\n📁 Files Loaded: {len(files)}")
        for file, cnt, first_load in files:
            logger.info(f"  {file}: {cnt:,} rows (loaded: {first_load})")

    except Exception as e:
        logger.error(f"Error during validation: {e}")

    logger.info("=" * 60 + "\n")


@retry(max_attempts=3, delay=5)
def load_csv_to_clickhouse(
    csv_path, target="dev", batch_size=10000, skip_duplicates=True
):
    """Load CSV file to ClickHouse in batches."""
    logger.info(f"Loading data from {csv_path}")

    client = get_clickhouse_client(target)
    filename = Path(csv_path).name

    # Check if already loaded
    if skip_duplicates and check_file_already_loaded(client, filename):
        logger.info(f"✓ File {filename} already loaded. Skipping.")
        return 0

    # Count total rows for progress bar
    total_rows_file = sum(1 for _ in open(csv_path)) - 1  # -1 for header

    chunk_count = 0
    total_rows = 0

    try:
        with tqdm(
            total=total_rows_file, desc=f"Loading {filename}", unit="rows"
        ) as pbar:
            for chunk in pd.read_csv(csv_path, chunksize=batch_size, low_memory=False):
                chunk_count += 1

                # Add source file tracking
                chunk["source_file"] = filename
                chunk["loaded_at"] = pd.Timestamp.now()

                # Clean the data
                chunk_clean = BikeShareCleaner.clean(chunk)

                if len(chunk_clean) == 0:
                    logger.warning(f"Chunk {chunk_count} is empty after cleaning")
                    pbar.update(len(chunk))
                    continue

                # Insert into ClickHouse
                try:
                    client.insert_df(table="raw_rides", df=chunk_clean)
                except Exception as e:
                    # Retry logic handles connection issues, but if table doesn't exist, create it?
                    # The check_file_already_loaded handles creation now.
                    raise e

                total_rows += len(chunk_clean)
                pbar.update(len(chunk))

        logger.info(f"✓ Successfully loaded {total_rows:,} rows from {filename}")
        return total_rows

    except Exception as e:
        logger.error(f"Error processing {csv_path}: {e}")
        raise


def load_directory(data_dir, target="dev", pattern="*.csv"):
    """Load all CSV files from a directory."""
    data_path = Path(data_dir)

    if not data_path.exists():
        logger.error(f"Directory does not exist: {data_dir}")
        sys.exit(1)

    csv_files = sorted(list(data_path.glob(pattern)))

    if not csv_files:
        logger.warning(f"No CSV files found in {data_dir}")
        return 0

    logger.info(f"Found {len(csv_files)} CSV files to load into {target}")

    total_loaded = 0
    failed_files = []

    for csv_file in csv_files:
        try:
            rows = load_csv_to_clickhouse(csv_file, target=target)
            total_loaded += rows
        except Exception as e:
            logger.error(f"Failed to load {csv_file}: {e}")
            failed_files.append(csv_file)

    logger.info(f"Total rows loaded: {total_loaded}")

    if failed_files:
        logger.warning(f"Failed to load {len(failed_files)} files:")
        for f in failed_files:
            logger.warning(f"  - {f}")

    return total_loaded


def download_and_load(limit=None, data_dir="./data/raw", target="dev"):
    """Download data from S3 and load to ClickHouse."""
    logger.info("Starting download from S3...")

    downloader = S3BikeShareDownloader(data_dir=data_dir)
    csv_files = downloader.download_all(limit=limit)

    if not csv_files:
        logger.error("No files downloaded")
        return 0

    logger.info(
        f"Downloaded {len(csv_files)} files, now loading to ClickHouse ({target})..."
    )

    total_loaded = 0
    for csv_file in csv_files:
        try:
            rows = load_csv_to_clickhouse(csv_file, target=target)
            total_loaded += rows
        except Exception as e:
            logger.error(f"Failed to load {csv_file}: {e}")

    return total_loaded


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load bike share CSV data to ClickHouse"
    )
    parser.add_argument("--file", type=str, help="Single CSV file to load")
    parser.add_argument(
        "--dir", type=str, default="./data/raw", help="Directory containing CSV files"
    )
    parser.add_argument(
        "--download", action="store_true", help="Download files from S3 before loading"
    )
    parser.add_argument(
        "--limit", type=int, help="Limit number of files to download (for testing)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=10000, help="Batch size for inserts"
    )
    # Added target argument
    parser.add_argument(
        "--target",
        type=str,
        default="dev",
        choices=["dev", "cloud"],
        help="Target environment: 'dev' (local) or 'cloud'",
    )

    args = parser.parse_args()

    try:
        if args.download:
            total = download_and_load(
                limit=args.limit, data_dir=args.dir, target=args.target
            )
        elif args.file:
            total = load_csv_to_clickhouse(
                args.file, target=args.target, batch_size=args.batch_size
            )
        else:
            total = load_directory(args.dir, target=args.target)

        # Validation
        if total >= 0:  # Check even if 0 to show empty state
            client = get_clickhouse_client(target=args.target)
            validate_loaded_data(client)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        sys.exit(1)
