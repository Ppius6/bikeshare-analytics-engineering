"""ClickHouse data loader module."""

import logging
import os
from typing import Any, Dict, Optional

import clickhouse_connect
import pandas as pd

logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """Handle ClickHouse data loading operations."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = None,
    ):
        """Initialize ClickHouse connection."""
        self.host = host or os.getenv("CLICKHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("CLICKHOUSE_PORT", "8123"))
        self.user = user or os.getenv("CLICKHOUSE_USER", "default")
        self.password = password or os.getenv("CLICKHOUSE_PASSWORD", "clickhouse")
        self.database = database or os.getenv("CLICKHOUSE_DB", "bikeshare")
        self.client = None

    def connect(self):
        """Establish connection to ClickHouse."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
            )
            logger.info("Connected to ClickHouse")
            return self.client
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def insert_dataframe(
        self, df: pd.DataFrame, table: str, batch_size: int = 10000
    ) -> int:
        """Insert DataFrame into ClickHouse table."""
        if self.client is None:
            self.connect()

        total_inserted = 0

        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]
            self.client.insert_df(table=table, df=batch)
            total_inserted += len(batch)
            logger.info(f"Inserted {total_inserted}/{len(df)} rows")

        return total_inserted

    def execute_query(self, query: str) -> Any:
        """Execute a query and return results."""
        if self.client is None:
            self.connect()

        return self.client.query(query)

    def close(self):
        """Close the connection."""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")
