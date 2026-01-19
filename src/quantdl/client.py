"""QuantDL client - main entry point for financial data access."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import TYPE_CHECKING

import polars as pl

from quantdl.data.calendar_master import CalendarMaster
from quantdl.data.security_master import SecurityMaster
from quantdl.exceptions import DataNotFoundError
from quantdl.storage.cache import DiskCache
from quantdl.storage.s3 import S3StorageBackend
from quantdl.types import SecurityInfo

if TYPE_CHECKING:
    from collections.abc import Sequence


class QuantDLClient:
    """Client for fetching financial data from S3 with local caching.

    Example:
        ```python
        client = QuantDLClient()

        # Get daily prices as wide table
        prices = client.ticks(["AAPL", "MSFT", "GOOGL"], "close", "2024-01-01", "2024-12-31")

        # Get fundamentals
        fundamentals = client.fundamentals(["AAPL"], "Revenue", "2024-01-01", "2024-12-31")
        ```
    """

    def __init__(
        self,
        bucket: str = "us-equity-datalake",
        cache_dir: str | None = None,
        cache_ttl_seconds: int | None = None,
        cache_max_size_bytes: int | None = None,
        max_concurrency: int = 10,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_region: str | None = None,
        local_data_path: str | None = None,
    ) -> None:
        """Initialize QuantDL client.

        Args:
            bucket: S3 bucket name
            cache_dir: Local cache directory (default: ~/.quantdl/cache)
            cache_ttl_seconds: Cache TTL in seconds (default: 24 hours)
            cache_max_size_bytes: Max cache size in bytes (default: 10GB)
            max_concurrency: Max concurrent S3 requests (default: 10)
            aws_access_key_id: AWS access key (default: from environment)
            aws_secret_access_key: AWS secret key (default: from environment)
            aws_region: AWS region (default: from environment or us-east-1)
            local_data_path: Local path for data files (for testing, bypasses S3)
        """
        self._storage = S3StorageBackend(
            bucket=bucket,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_region=aws_region,
            local_path=local_data_path,
        )

        self._cache = DiskCache(
            cache_dir=cache_dir,
            ttl_seconds=cache_ttl_seconds,
            max_size_bytes=cache_max_size_bytes,
        )

        self._security_master = SecurityMaster(self._storage, self._cache)
        self._calendar_master = CalendarMaster(self._storage, self._cache)
        self._max_concurrency = max_concurrency
        self._executor = ThreadPoolExecutor(max_workers=max_concurrency)

    @property
    def security_master(self) -> SecurityMaster:
        """Access security master for direct lookups."""
        return self._security_master

    @property
    def calendar_master(self) -> CalendarMaster:
        """Access calendar master for trading day lookups."""
        return self._calendar_master

    def resolve(self, identifier: str, as_of: date | None = None) -> SecurityInfo | None:
        """Resolve symbol/identifier to SecurityInfo.

        Args:
            identifier: Symbol, CIK, security_id, or permno
            as_of: Point-in-time date (default: today)

        Returns:
            SecurityInfo if found, None otherwise
        """
        return self._security_master.resolve(identifier, as_of)

    def _resolve_securities(
        self,
        symbols: Sequence[str],
        as_of: date | None = None,
    ) -> list[tuple[str, SecurityInfo]]:
        """Resolve symbols and return list of (symbol, info) pairs."""
        result: list[tuple[str, SecurityInfo]] = []
        for sym in symbols:
            info = self._security_master.resolve(sym, as_of)
            if info is not None:
                result.append((sym, info))
        return result

    def _align_to_calendar(self, wide: pl.DataFrame, start: date, end: date) -> pl.DataFrame:
        """Align wide table rows to trading calendar."""
        trading_days = self._calendar_master.get_trading_days(start, end)
        calendar_df = pl.DataFrame({"timestamp": trading_days})
        return calendar_df.join(wide, on="timestamp", how="left").sort("timestamp")

    def _fetch_ticks_single(
        self,
        security_id: str,
        start: date,
        end: date,
    ) -> pl.DataFrame | None:
        """Fetch daily ticks for single security."""
        path = f"data/raw/ticks/daily/{security_id}/history.parquet"

        # Try cache first
        cached = self._cache.get(path)
        if cached is not None:
            return cached.filter(
                (pl.col("timestamp") >= start) & (pl.col("timestamp") <= end)
            )

        # Fetch from S3
        try:
            df = self._storage.read_parquet(path)
            # Cast timestamp to date if it's a string
            if df.schema["timestamp"] == pl.String:
                df = df.with_columns(pl.col("timestamp").str.to_date())
            # Filter by date range
            df = df.filter(
                (pl.col("timestamp") >= start) & (pl.col("timestamp") <= end)
            )
            # Cache full file for future use
            self._cache.put(path, df)
            return df
        except Exception:
            return None

    async def _fetch_ticks_async(
        self,
        securities: list[tuple[str, SecurityInfo]],
        start: date,
        end: date,
    ) -> list[tuple[str, pl.DataFrame]]:
        """Fetch daily data for multiple securities concurrently."""
        loop = asyncio.get_event_loop()
        tasks = []

        for symbol, info in securities:
            task = loop.run_in_executor(
                self._executor,
                self._fetch_ticks_single,
                info.security_id,
                start,
                end,
            )
            tasks.append((symbol, task))

        results: list[tuple[str, pl.DataFrame]] = []
        for symbol, task in tasks:
            df = await task
            if df is not None and len(df) > 0:
                results.append((symbol, df))

        return results

    def ticks(
        self,
        symbols: Sequence[str] | str,
        field: str = "close",
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> pl.DataFrame:
        """Get daily price data as wide table.

        Args:
            symbols: Symbol(s) to fetch
            field: Price field (open, high, low, close, volume)
            start: Start date
            end: End date (default: today)

        Returns:
            Wide DataFrame with timestamp as first column, symbols as other columns

        Example:
            ```python
            # Returns DataFrame with columns: timestamp, AAPL, MSFT, GOOGL
            prices = client.ticks(["AAPL", "MSFT", "GOOGL"], "close")
            ```
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        # Parse dates
        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end, str):
            end = date.fromisoformat(end)

        start = start or date(2000, 1, 1)
        end = end or date.today() - timedelta(days=1)

        # Resolve symbols to security IDs
        resolved = self._resolve_securities(symbols, as_of=start)
        if not resolved:
            raise DataNotFoundError("ticks", ", ".join(symbols))

        # Fetch data concurrently
        results = asyncio.run(self._fetch_ticks_async(resolved, start, end))

        if not results:
            raise DataNotFoundError("ticks", ", ".join(symbols))

        # Build wide table
        dfs: list[pl.DataFrame] = []
        for symbol, df in results:
            if field not in df.columns:
                continue
            dfs.append(
                df.select(
                    pl.col("timestamp"),
                    pl.lit(symbol).alias("symbol"),
                    pl.col(field).alias("value"),
                )
            )

        if not dfs:
            raise DataNotFoundError("ticks", f"field={field}")

        # Concat and pivot
        combined = pl.concat(dfs)
        wide = combined.pivot(values="value", index="timestamp", on="symbol")

        # Align to trading calendar
        return self._align_to_calendar(wide, start, end)

    def _fetch_fundamentals_single(
        self,
        cik: str,
        start: date,
        end: date,
    ) -> pl.DataFrame | None:
        """Fetch fundamentals for single security by CIK."""
        path = f"data/raw/fundamental/{cik}/fundamental.parquet"

        # Try cache first
        cached = self._cache.get(path)
        if cached is not None:
            return cached.filter(
                (pl.col("as_of_date") >= start) & (pl.col("as_of_date") <= end)
            )

        try:
            df = self._storage.read_parquet(path)
            # Cast as_of_date to date if it's a string
            if df.schema["as_of_date"] == pl.String:
                df = df.with_columns(pl.col("as_of_date").str.to_date())
            self._cache.put(path, df)
            return df.filter(
                (pl.col("as_of_date") >= start) & (pl.col("as_of_date") <= end)
            )
        except Exception:
            return None

    async def _fetch_fundamentals_async(
        self,
        securities: list[tuple[str, SecurityInfo]],
        start: date,
        end: date,
    ) -> list[tuple[str, pl.DataFrame]]:
        """Fetch fundamentals for multiple securities concurrently."""
        loop = asyncio.get_event_loop()
        tasks = []

        for symbol, info in securities:
            if info.cik is None:
                continue
            task = loop.run_in_executor(
                self._executor,
                self._fetch_fundamentals_single,
                info.cik,
                start,
                end,
            )
            tasks.append((symbol, task))

        results: list[tuple[str, pl.DataFrame]] = []
        for symbol, task in tasks:
            df = await task
            if df is not None and len(df) > 0:
                results.append((symbol, df))

        return results

    def fundamentals(
        self,
        symbols: Sequence[str] | str,
        concept: str,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> pl.DataFrame:
        """Get fundamental data as wide table.

        Args:
            symbols: Symbol(s) to fetch
            concept: Fundamental concept (e.g., "Revenue", "NetIncome")
            start: Start date
            end: End date

        Returns:
            Wide DataFrame with as_of_date as first column, symbols as other columns
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end, str):
            end = date.fromisoformat(end)

        start = start or date(2000, 1, 1)
        end = end or date.today() - timedelta(days=1)

        resolved = self._resolve_securities(symbols, as_of=start)
        if not resolved:
            raise DataNotFoundError("fundamentals", ", ".join(symbols))

        results = asyncio.run(self._fetch_fundamentals_async(resolved, start, end))

        if not results:
            raise DataNotFoundError("fundamentals", ", ".join(symbols))

        # Filter by concept and build wide table
        dfs: list[pl.DataFrame] = []
        for symbol, df in results:
            filtered = df.filter(pl.col("concept") == concept)
            if len(filtered) > 0:
                dfs.append(
                    filtered.select(
                        pl.col("as_of_date").alias("timestamp"),
                        pl.lit(symbol).alias("symbol"),
                        pl.col("value"),
                    )
                )

        if not dfs:
            raise DataNotFoundError("fundamentals", f"concept={concept}")

        combined = pl.concat(dfs)
        # Deduplicate: take first value per (timestamp, symbol)
        combined = combined.group_by(["timestamp", "symbol"]).agg(pl.col("value").first())
        wide = combined.pivot(values="value", index="timestamp", on="symbol")
        return self._align_to_calendar(wide, start, end)

    def _fetch_metrics_single(
        self,
        cik: str,
        start: date,
        end: date,
    ) -> pl.DataFrame | None:
        """Fetch metrics for single security by CIK."""
        path = f"data/derived/features/fundamental/{cik}/metrics.parquet"

        cached = self._cache.get(path)
        if cached is not None:
            return cached.filter(
                (pl.col("as_of_date") >= start) & (pl.col("as_of_date") <= end)
            )

        try:
            df = self._storage.read_parquet(path)
            self._cache.put(path, df)
            return df.filter(
                (pl.col("as_of_date") >= start) & (pl.col("as_of_date") <= end)
            )
        except Exception:
            return None

    async def _fetch_metrics_async(
        self,
        securities: list[tuple[str, SecurityInfo]],
        start: date,
        end: date,
    ) -> list[tuple[str, pl.DataFrame]]:
        """Fetch metrics for multiple securities concurrently."""
        loop = asyncio.get_event_loop()
        tasks = []

        for symbol, info in securities:
            if info.cik is None:
                continue
            task = loop.run_in_executor(
                self._executor,
                self._fetch_metrics_single,
                info.cik,
                start,
                end,
            )
            tasks.append((symbol, task))

        results: list[tuple[str, pl.DataFrame]] = []
        for symbol, task in tasks:
            df = await task
            if df is not None and len(df) > 0:
                results.append((symbol, df))

        return results

    def metrics(
        self,
        symbols: Sequence[str] | str,
        metric: str,
        start: date | str | None = None,
        end: date | str | None = None,
    ) -> pl.DataFrame:
        """Get derived metrics as wide table.

        Args:
            symbols: Symbol(s) to fetch
            metric: Metric name (e.g., "pe_ratio", "pb_ratio", "roe", "roa")
            start: Start date
            end: End date

        Returns:
            Wide DataFrame with as_of_date as first column, symbols as other columns
        """
        if isinstance(symbols, str):
            symbols = [symbols]

        if isinstance(start, str):
            start = date.fromisoformat(start)
        if isinstance(end, str):
            end = date.fromisoformat(end)

        start = start or date(2000, 1, 1)
        end = end or date.today() - timedelta(days=1)

        resolved = self._resolve_securities(symbols, as_of=start)
        if not resolved:
            raise DataNotFoundError("metrics", ", ".join(symbols))

        results = asyncio.run(self._fetch_metrics_async(resolved, start, end))

        if not results:
            raise DataNotFoundError("metrics", ", ".join(symbols))

        dfs: list[pl.DataFrame] = []
        for symbol, df in results:
            if metric not in df.columns:
                continue
            dfs.append(
                df.select(
                    pl.col("as_of_date").alias("timestamp"),
                    pl.lit(symbol).alias("symbol"),
                    pl.col(metric).alias("value"),
                )
            )

        if not dfs:
            raise DataNotFoundError("metrics", f"metric={metric}")

        combined = pl.concat(dfs)
        wide = combined.pivot(values="value", index="timestamp", on="symbol")
        return self._align_to_calendar(wide, start, end)

    def universe(self, name: str = "top3000") -> list[str]:
        """Load universe of symbols.

        Args:
            name: Universe name (default: "top3000")

        Returns:
            List of symbols in the universe
        """
        path = f"data/universe/{name}.parquet"

        cached = self._cache.get(path)
        if cached is not None:
            return cached["symbol"].to_list()

        try:
            df = self._storage.read_parquet(path)
            self._cache.put(path, df)
            return df["symbol"].to_list()
        except Exception as e:
            raise DataNotFoundError("universe", name) from e

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()

    def cache_stats(self) -> dict[str, object]:
        """Get cache statistics."""
        return self._cache.stats()

    def close(self) -> None:
        """Clean up resources."""
        self._executor.shutdown(wait=False)

    def __enter__(self) -> QuantDLClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
