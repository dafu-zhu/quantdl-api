"""AlphaSession: Unified session for alpha research with S3 data."""

from __future__ import annotations

import ast
import threading
from collections.abc import Sequence
from datetime import date
from typing import TYPE_CHECKING, Any

import polars as pl

import quantdl.operators as ops
from quantdl.alpha.core import Alpha
from quantdl.alpha.parser import AlphaParseError, SafeEvaluator
from quantdl.alpha.types import DataSpec
from quantdl.alpha.validation import FieldNotFoundError, SessionNotActiveError

if TYPE_CHECKING:
    from quantdl.client import QuantDLClient

# Default field aliases mapping name -> DataSpec
FIELD_ALIASES: dict[str, DataSpec] = {
    # Daily OHLCV
    "open": DataSpec("daily", "open"),
    "high": DataSpec("daily", "high"),
    "low": DataSpec("daily", "low"),
    "close": DataSpec("daily", "close"),
    "volume": DataSpec("daily", "volume"),
    # Shortcuts
    "price": DataSpec("daily", "close"),
    # Fundamentals
    "revenue": DataSpec("fundamentals", "Revenue"),
    "net_income": DataSpec("fundamentals", "NetIncome"),
    # Metrics
    "pe": DataSpec("metrics", "pe_ratio"),
    "pb": DataSpec("metrics", "pb_ratio"),
}


class AlphaSession:
    """Unified session for alpha research with automatic data fetching.

    Provides lazy/eager data access with caching, chunking for large universes,
    and thread-safe concurrent access.

    Example:
        >>> with AlphaSession(client, ["AAPL", "MSFT"], "2024-01-01", "2024-12-31") as s:
        ...     close = s.close   # Lazy fetch, returns Alpha
        ...     vol = s.volume    # Cached on first access
        ...     signal = close / Alpha(ops.ts_delay(close.data, 1)) - 1
    """

    __slots__ = (
        "_client",
        "_symbols",
        "_start",
        "_end",
        "_eager",
        "_fields",
        "_chunk_size",
        "_cache",
        "_custom_specs",
        "_active",
        "_lock",
    )

    def __init__(
        self,
        client: QuantDLClient,
        symbols: Sequence[str],
        start: date | str,
        end: date | str,
        *,
        eager: bool = False,
        fields: Sequence[str] | None = None,
        chunk_size: int | None = None,
    ) -> None:
        """Initialize AlphaSession.

        Args:
            client: QuantDLClient for data fetching
            symbols: Symbols to include in session
            start: Start date (inclusive)
            end: End date (inclusive)
            eager: If True, prefetch specified fields on __enter__
            fields: Fields to prefetch if eager=True
            chunk_size: Max symbols per fetch (None = no chunking)
        """
        self._client = client
        self._symbols = list(symbols)
        self._start = date.fromisoformat(start) if isinstance(start, str) else start
        self._end = date.fromisoformat(end) if isinstance(end, str) else end
        self._eager = eager
        self._fields = list(fields) if fields else []
        self._chunk_size = chunk_size
        self._cache: dict[str, Alpha] = {}
        self._custom_specs: dict[str, DataSpec] = {}
        self._active = False
        self._lock = threading.Lock()

    def _resolve_spec(self, name: str) -> DataSpec:
        """Resolve field name to DataSpec."""
        if name in self._custom_specs:
            return self._custom_specs[name]
        if name in FIELD_ALIASES:
            return FIELD_ALIASES[name]
        raise FieldNotFoundError(name)

    def _fetch_single_chunk(self, spec: DataSpec, symbols: Sequence[str]) -> pl.DataFrame:
        """Fetch data for a single chunk of symbols."""
        if spec.source == "daily":
            return self._client.daily(symbols, spec.field, self._start, self._end)
        elif spec.source == "fundamentals":
            return self._client.fundamentals(symbols, spec.field, self._start, self._end)
        elif spec.source == "metrics":
            return self._client.metrics(symbols, spec.field, self._start, self._end)
        else:
            raise ValueError(f"Unknown source: {spec.source}")

    def _fetch_field(self, name: str) -> Alpha:
        """Fetch field data, using chunking if configured."""
        spec = self._resolve_spec(name)

        if self._chunk_size is None or len(self._symbols) <= self._chunk_size:
            df = self._fetch_single_chunk(spec, self._symbols)
            return Alpha(df)

        # Chunked fetch
        chunks: list[pl.DataFrame] = []
        for i in range(0, len(self._symbols), self._chunk_size):
            chunk_symbols = self._symbols[i : i + self._chunk_size]
            chunk_df = self._fetch_single_chunk(spec, chunk_symbols)
            chunks.append(chunk_df)

        # Combine chunks by joining on timestamp
        if not chunks:
            raise ValueError("No data fetched")

        result = chunks[0]
        for chunk in chunks[1:]:
            # Join preserving all symbol columns
            result = result.join(chunk, on="timestamp", how="full", coalesce=True)

        # Reorder columns: timestamp first, then symbols in original order
        symbol_cols = [s for s in self._symbols if s in result.columns]
        result = result.select(["timestamp", *symbol_cols])

        return Alpha(result)

    def __getattr__(self, name: str) -> Alpha:
        """Lazy fetch field on attribute access."""
        if name.startswith("_"):
            raise AttributeError(name)

        if not self._active:
            raise SessionNotActiveError()

        with self._lock:
            if name in self._cache:
                return self._cache[name]
            alpha = self._fetch_field(name)
            self._cache[name] = alpha
            return alpha

    def fetch(self, *fields: str) -> dict[str, Alpha]:
        """Batch fetch multiple fields.

        Args:
            *fields: Field names to fetch

        Returns:
            Dict mapping field name to Alpha
        """
        if not self._active:
            raise SessionNotActiveError()

        result: dict[str, Alpha] = {}
        for field in fields:
            with self._lock:
                if field in self._cache:
                    result[field] = self._cache[field]
                else:
                    alpha = self._fetch_field(field)
                    self._cache[field] = alpha
                    result[field] = alpha
        return result

    def eval(self, expr: str) -> Alpha:
        """Evaluate alpha expression string.

        Variables in the expression are resolved lazily from session.

        Args:
            expr: Expression string, e.g., "ops.rank(-ops.ts_delta(close, 5))"

        Returns:
            Alpha with computed result
        """
        if not self._active:
            raise SessionNotActiveError()

        try:
            tree = ast.parse(expr, mode="eval")
        except SyntaxError as e:
            raise AlphaParseError(f"Invalid expression syntax: {e}") from e

        # Create proxy that fetches from session on variable access
        variables = _SessionVariableProxy(self)
        evaluator = SafeEvaluator(variables, ops)
        result = evaluator.visit(tree)

        if isinstance(result, Alpha):
            return result
        if isinstance(result, pl.DataFrame):
            return Alpha(result)
        raise AlphaParseError(f"Expression did not return Alpha/DataFrame: {type(result)}")

    def register(self, name: str, spec: DataSpec) -> None:
        """Register custom field mapping.

        Args:
            name: Field name to register
            spec: DataSpec defining how to fetch the field
        """
        self._custom_specs[name] = spec

    def __enter__(self) -> AlphaSession:
        self._active = True
        if self._eager and self._fields:
            self.fetch(*self._fields)
        return self

    def __exit__(self, *args: object) -> None:
        self._active = False
        self._cache.clear()


class _SessionVariableProxy(dict[str, Any]):
    """Proxy dict that fetches from AlphaSession on missing key access."""

    def __init__(self, session: AlphaSession) -> None:
        super().__init__()
        self._session = session

    def __getitem__(self, key: str) -> Any:
        if key == "ops":
            return ops
        # Fetch from session (returns Alpha)
        return getattr(self._session, key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key == "ops":
            return True
        try:
            self._session._resolve_spec(key)
            return True
        except FieldNotFoundError:
            return False
