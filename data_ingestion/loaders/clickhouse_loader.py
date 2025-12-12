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
        secure: bool = None,
    ):
        """
        Initialize ClickHouse connection.
        Defaults to CLICKHOUSE_CLOUD_* variables from .env
        """
        self.host = host or os.getenv("CLICKHOUSE_CLOUD_HOST")
        self.port = port or int(os.getenv("CLICKHOUSE_CLOUD_HTTP_PORT", "8443"))
        self.user = user or os.getenv("CLICKHOUSE_CLOUD_USER", "default")
        self.password = password or os.getenv("CLICKHOUSE_CLOUD_PASSWORD")
        self.database = database or os.getenv("CLICKHOUSE_CLOUD_DB", "default")

        # Cloud is always secure (SSL)
        if secure is not None:
            self.secure = secure
        else:
            # Default to True for cloud, or check env var
            self.secure = True

        self.client = None

        # Safety check to prevent running against localhost when expecting cloud
        if not self.host or "localhost" in self.host:
            logger.warning(
                "⚠️  Warning: Host appears to be local. Check your CLICKHOUSE_CLOUD_HOST variable."
            )

    def connect(self):
        """Establish connection to ClickHouse."""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.user,
                password=self.password,
                database=self.database,
                secure=self.secure,
                send_receive_timeout=300,
            )

            # Mask password in logs
            masked_host = self.host[:15] + "..." if self.host else "None"
            logger.info(
                f"✅ Connected to ClickHouse Cloud at {masked_host}:{self.port}"
            )
            return self.client
        except Exception as e:
            logger.error(f"❌ Failed to connect to ClickHouse: {e}")
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
            try:
                self.client.insert_df(table=table, df=batch, database=self.database)
                total_inserted += len(batch)
                logger.info(
                    f"   ↳ Inserted batch: {len(batch)} rows (Total: {total_inserted})"
                )
            except Exception as e:
                logger.error(f"   ↳ Error inserting batch at index {i}: {e}")
                raise

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
