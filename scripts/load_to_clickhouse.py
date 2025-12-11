#!/usr/bin/env python3
"""
Load bike share CSV data into ClickHouse.
Usage: python load_to_clickhouse.py --file data.csv
       python load_to_clickhouse.py --dir ./data/raw
       python load_to_clickhouse.py --download --limit 5
"""

import argparse
import logging
import os
import sys
import time
from functools import wraps
from pathlib import Path

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
        result = client.query(
            f"SELECT count(*) FROM raw_rides WHERE source_file = '{filename}'"
        ).result_rows
        return result[0][0] > 0
    except:
        # Column might not exist in older schemas
        return False


@retry(max_attempts=3, delay=5)
def get_clickhouse_client():
    """Create ClickHouse client connection."""
    try:
        client = clickhouse_connect.get_client(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            username=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
            database=os.getenv("CLICKHOUSE_DB", "bikeshare"),
        )
        logger.info("Successfully connected to ClickHouse")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to ClickHouse: {e}")
        raise


def validate_loaded_data(client):
    """Generate data quality report after loading."""
    logger.info("\n" + "=" * 60)
    logger.info("DATA VALIDATION REPORT")
    logger.info("=" * 60)

    # Total records
    total = client.query("SELECT count(*) FROM raw_rides").result_rows[0][0]
    logger.info(f"📊 Total records: {total:,}")

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
            countIf(ride_id IS NULL) as null_ride_id,
            countIf(started_at IS NULL) as null_started_at,
            countIf(ended_at IS NULL) as null_ended_at,
            countIf(start_station_id IS NULL) as null_start_station,
            countIf(end_station_id IS NULL) as null_end_station
        FROM raw_rides
    """
    ).result_rows[0]

    logger.info(f"\n🔍 Data Quality Checks:")
    logger.info(f"  Null ride_id: {nulls[0]:,}")
    logger.info(f"  Null started_at: {nulls[1]:,}")
    logger.info(f"  Null ended_at: {nulls[2]:,}")
    logger.info(f"  Null start_station: {nulls[3]:,}")
    logger.info(f"  Null end_station: {nulls[4]:,}")

    # User type breakdown
    user_types = client.query(
        """
        SELECT member_casual, count(*) as cnt
        FROM raw_rides
        GROUP BY member_casual
        ORDER BY cnt DESC
    """
    ).result_rows

    logger.info(f"\n👥 User Type Breakdown:")
    for user_type, cnt in user_types:
        logger.info(f"  {user_type}: {cnt:,}")

    # Files loaded
    try:
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
    except:
        logger.info(f"\n📁 Source file tracking not available")

    logger.info("=" * 60 + "\n")


@retry(max_attempts=3, delay=5)
def load_csv_to_clickhouse(csv_path, batch_size=10000, skip_duplicates=True):
    """Load CSV file to ClickHouse in batches."""
    logger.info(f"Loading data from {csv_path}")

    client = get_clickhouse_client()
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
                client.insert_df(table="raw_rides", df=chunk_clean)

                total_rows += len(chunk_clean)
                pbar.update(len(chunk))

        logger.info(f"✓ Successfully loaded {total_rows:,} rows from {filename}")
        return total_rows

    except Exception as e:
        logger.error(f"Error processing {csv_path}: {e}")
        raise


def load_directory(data_dir, pattern="*.csv"):
    """Load all CSV files from a directory."""
    data_path = Path(data_dir)

    if not data_path.exists():
        logger.error(f"Directory does not exist: {data_dir}")
        sys.exit(1)

    csv_files = sorted(list(data_path.glob(pattern)))

    if not csv_files:
        logger.warning(f"No CSV files found in {data_dir}")
        return 0

    logger.info(f"Found {len(csv_files)} CSV files to load")

    total_loaded = 0
    failed_files = []

    for csv_file in csv_files:
        try:
            rows = load_csv_to_clickhouse(csv_file)
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


def download_and_load(limit=None, data_dir="./data/raw"):
    """Download data from S3 and load to ClickHouse."""
    logger.info("Starting download from S3...")

    downloader = S3BikeShareDownloader(data_dir=data_dir)
    csv_files = downloader.download_all(limit=limit)

    if not csv_files:
        logger.error("No files downloaded")
        return 0

    logger.info(f"Downloaded {len(csv_files)} files, now loading to ClickHouse...")

    total_loaded = 0
    for csv_file in csv_files:
        try:
            rows = load_csv_to_clickhouse(csv_file)
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

    args = parser.parse_args()

    try:
        if args.download:
            total = download_and_load(limit=args.limit, data_dir=args.dir)
            logger.info(f"Pipeline complete. Total rows loaded: {total:,}")

            # Add validation report
            if total > 0:
                client = get_clickhouse_client()
                validate_loaded_data(client)

        elif args.file:
            total = load_csv_to_clickhouse(args.file, args.batch_size)
            client = get_clickhouse_client()
            validate_loaded_data(client)
        else:
            total = load_directory(args.dir)
            client = get_clickhouse_client()
            validate_loaded_data(client)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        sys.exit(1)
