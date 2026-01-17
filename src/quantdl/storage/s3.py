"""S3 storage backend using Polars native scan_parquet."""

import os
from pathlib import Path
from typing import Any

import polars as pl

from quantdl.exceptions import S3Error


class S3StorageBackend:
    """S3 storage backend using Polars' native object_store integration.

    Can operate in two modes:
    1. S3 mode (default): Reads from S3 using polars' object_store
    2. Local mode: Reads from local filesystem (for testing)
    """

    def __init__(
        self,
        bucket: str = "us-equity-datalake",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_region: str | None = None,
        local_path: str | Path | None = None,
    ) -> None:
        """Initialize storage backend.

        Args:
            bucket: S3 bucket name
            aws_access_key_id: AWS access key
            aws_secret_access_key: AWS secret key
            aws_region: AWS region
            local_path: If provided, read from local filesystem instead of S3
        """
        self.bucket = bucket
        self._local_path = Path(local_path) if local_path else None
        self._storage_options: dict[str, str] = {}

        if not self._local_path:
            # Use provided credentials or fall back to environment
            access_key = aws_access_key_id or os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = aws_secret_access_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
            region = aws_region or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

            if access_key:
                self._storage_options["aws_access_key_id"] = access_key
            if secret_key:
                self._storage_options["aws_secret_access_key"] = secret_key
            if region:
                self._storage_options["aws_region"] = region

    def _resolve_path(self, path: str) -> str:
        """Resolve path to URI or local path."""
        if self._local_path:
            return str(self._local_path / path.lstrip("/"))
        return f"s3://{self.bucket}/{path.lstrip('/')}"

    def scan_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
    ) -> pl.LazyFrame:
        """Scan parquet file as LazyFrame.

        Args:
            path: Path within bucket (e.g., "data/master/security_master.parquet")
            columns: Optional list of columns to select

        Returns:
            LazyFrame for lazy evaluation with predicate pushdown
        """
        resolved = self._resolve_path(path)
        try:
            if self._local_path:
                lf = pl.scan_parquet(resolved)
            else:
                lf = pl.scan_parquet(resolved, storage_options=self._storage_options)
            if columns:
                lf = lf.select(columns)
            return lf
        except Exception as e:
            raise S3Error("scan_parquet", path, e) from e

    def read_parquet(
        self,
        path: str,
        columns: list[str] | None = None,
        filters: list[Any] | None = None,
    ) -> pl.DataFrame:
        """Read parquet file into DataFrame.

        Args:
            path: Path within bucket
            columns: Optional list of columns to select
            filters: Optional list of filter expressions to apply

        Returns:
            DataFrame with data
        """
        lf = self.scan_parquet(path, columns)
        if filters:
            for f in filters:
                lf = lf.filter(f)
        try:
            return lf.collect()
        except Exception as e:
            raise S3Error("read_parquet", path, e) from e

    def exists(self, path: str) -> bool:
        """Check if a path exists.

        Note: This attempts to scan schema which is lightweight.
        """
        try:
            resolved = self._resolve_path(path)
            if self._local_path:
                return Path(resolved).exists()
            _ = self.scan_parquet(path).schema
            return True
        except S3Error:
            return False
