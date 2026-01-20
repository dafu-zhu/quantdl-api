"""S3 storage backend using Polars native scan_parquet."""

import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from quantdl.exceptions import S3Error

# Default path for persisting request counts
_DEFAULT_COUNTS_PATH = Path.home() / ".quantdl" / "request_counts.json"


class S3RequestCounter:
    """Tracks S3 API request counts per session and per day.

    Daily counts persist across sessions via JSON file.
    Session counts reset on each new client instance.
    """

    def __init__(self, persist_path: Path | None = None) -> None:
        """Initialize counter, loading persisted daily counts.

        Args:
            persist_path: Path to persist daily counts (default: ~/.quantdl/request_counts.json)
        """
        self._session_count = 0
        self._persist_path = persist_path or _DEFAULT_COUNTS_PATH
        self._daily_counts: dict[date, int] = self._load()

    def _load(self) -> dict[date, int]:
        """Load daily counts from disk."""
        if not self._persist_path.exists():
            return {}
        try:
            with open(self._persist_path) as f:
                data = json.load(f)
            return {date.fromisoformat(k): v for k, v in data.items()}
        except (json.JSONDecodeError, ValueError, OSError):
            return {}

    def _save(self) -> None:
        """Save daily counts to disk."""
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            data = {k.isoformat(): v for k, v in self._daily_counts.items()}
            with open(self._persist_path, "w") as f:
                json.dump(data, f)
        except OSError:
            pass  # Silently fail if unable to persist

    def increment(self) -> None:
        """Increment request count for session and current day."""
        self._session_count += 1
        today = date.today()
        self._daily_counts[today] = self._daily_counts.get(today, 0) + 1
        self._save()

    @property
    def session_count(self) -> int:
        """Total requests in this session."""
        return self._session_count

    @property
    def today_count(self) -> int:
        """Requests made today."""
        return self._daily_counts.get(date.today(), 0)

    def daily_count(self, day: date) -> int:
        """Requests made on a specific day."""
        return self._daily_counts.get(day, 0)

    def reset_session(self) -> None:
        """Reset session counter."""
        self._session_count = 0

    def reset_daily(self) -> None:
        """Reset all daily counts and remove persisted file."""
        self._daily_counts = {}
        if self._persist_path.exists():
            self._persist_path.unlink()

    def stats(self) -> dict[str, Any]:
        """Get all stats as dictionary."""
        return {
            "session_count": self._session_count,
            "today_count": self.today_count,
            "daily_counts": {k.isoformat(): v for k, v in self._daily_counts.items()},
        }


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
        self._request_counter = S3RequestCounter()

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
                self._request_counter.increment()
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

    @property
    def request_counter(self) -> S3RequestCounter:
        """Access the request counter."""
        return self._request_counter
