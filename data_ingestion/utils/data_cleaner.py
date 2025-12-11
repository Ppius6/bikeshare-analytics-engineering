"""Data cleaning utilities for bike share data."""

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class BikeShareCleaner:
    """Clean and validate bike share data."""

    # Required columns
    REQUIRED_COLUMNS = [
        "ride_id",
        "rideable_type",
        "started_at",
        "ended_at",
        "member_casual",
    ]

    # String columns that should never be float/NaN
    STRING_COLUMNS = [
        "ride_id",
        "rideable_type",
        "start_station_name",
        "start_station_id",
        "end_station_name",
        "end_station_id",
        "member_casual",
        "source_file",
    ]

    @staticmethod
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate a DataFrame of bike share rides."""
        if df.empty:
            return df

        original_count = len(df)

        # Make a copy to avoid modifying the original
        df = df.copy()

        # Convert string columns - replace NaN with empty string
        for col in BikeShareCleaner.STRING_COLUMNS:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)

        # Parse datetime columns
        datetime_cols = ["started_at", "ended_at"]
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Convert numeric columns
        numeric_cols = ["start_lat", "start_lng", "end_lat", "end_lng"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Remove rows with missing required fields
        for col in BikeShareCleaner.REQUIRED_COLUMNS:
            if col in df.columns:
                df = df[df[col].notna()]

        # Remove invalid datetime ranges
        if "started_at" in df.columns and "ended_at" in df.columns:
            df = df[df["started_at"] < df["ended_at"]]

        # Remove rides with duration < 60 seconds or > 24 hours
        if "started_at" in df.columns and "ended_at" in df.columns:
            duration = (df["ended_at"] - df["started_at"]).dt.total_seconds()
            df = df[(duration >= 60) & (duration <= 86400)]

        # Log cleaning results
        dropped = original_count - len(df)
        if dropped > 0:
            logger.warning(f"Dropped {dropped} rows due to validation rules")

        return df

    @staticmethod
    def validate_schema(df: pd.DataFrame) -> bool:
        """Validate that DataFrame has required columns."""
        missing = set(BikeShareCleaner.REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            logger.error(f"Missing required columns: {missing}")
            return False
        return True
