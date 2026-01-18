"""Tests for AlphaSession."""

import dataclasses
import threading
from pathlib import Path

import polars as pl
import pytest

from quantdl import QuantDLClient
from quantdl.alpha import (
    Alpha,
    AlphaSession,
    DataSpec,
    FieldNotFoundError,
    SessionNotActiveError,
)


@pytest.fixture
def client(test_data_dir: Path, temp_cache_dir: str) -> QuantDLClient:
    """Create client with local test data."""
    return QuantDLClient(
        bucket="us-equity-datalake",
        cache_dir=temp_cache_dir,
        local_data_path=str(test_data_dir),
    )


class TestAlphaSessionBasics:
    """Basic AlphaSession tests."""

    def test_context_manager(self, client: QuantDLClient) -> None:
        """Test session as context manager."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            assert s is not None

    def test_lazy_fetch(self, client: QuantDLClient) -> None:
        """Test lazy fetch returns Alpha."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            close = s.close
            assert isinstance(close, Alpha)
            assert "AAPL" in close.data.columns

    def test_caching(self, client: QuantDLClient) -> None:
        """Test field is cached after first access."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            close1 = s.close
            close2 = s.close
            assert close1 is close2  # Same object (cached)

    def test_multiple_fields(self, client: QuantDLClient) -> None:
        """Test accessing multiple fields."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            close = s.close
            volume = s.volume
            assert isinstance(close, Alpha)
            assert isinstance(volume, Alpha)
            assert close is not volume

    def test_field_alias(self, client: QuantDLClient) -> None:
        """Test price alias maps to close."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            price = s.price
            close = s.close
            assert isinstance(price, Alpha)
            # Both should have same data (though different Alpha objects due to separate fetches)
            assert price.data["AAPL"].to_list() == close.data["AAPL"].to_list()


class TestAlphaSessionFetch:
    """Tests for batch fetch."""

    def test_fetch_single(self, client: QuantDLClient) -> None:
        """Test batch fetch single field."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            result = s.fetch("close")
            assert "close" in result
            assert isinstance(result["close"], Alpha)

    def test_fetch_multiple(self, client: QuantDLClient) -> None:
        """Test batch fetch multiple fields."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            result = s.fetch("close", "volume", "open")
            assert len(result) == 3
            assert all(isinstance(v, Alpha) for v in result.values())

    def test_fetch_caches(self, client: QuantDLClient) -> None:
        """Test batch fetch populates cache."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            result = s.fetch("close")
            # Should be same object when accessed via attribute
            close = s.close
            assert close is result["close"]


class TestAlphaSessionEval:
    """Tests for eval() string DSL."""

    def test_eval_simple(self, client: QuantDLClient) -> None:
        """Test simple eval expression."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            alpha = s.eval("close + 10")
            assert isinstance(alpha, Alpha)

    def test_eval_with_ops(self, client: QuantDLClient) -> None:
        """Test eval with ops namespace."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            alpha = s.eval("ops.ts_delta(close, 1)")
            assert isinstance(alpha, Alpha)

    def test_eval_complex(self, client: QuantDLClient) -> None:
        """Test complex eval expression."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            alpha = s.eval("ops.rank(-ops.ts_delta(close, 1))")
            assert isinstance(alpha, Alpha)


class TestAlphaSessionRegister:
    """Tests for custom field registration."""

    def test_register_custom_field(self, client: QuantDLClient) -> None:
        """Test registering custom field."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            s.register("adjusted_close", DataSpec("ticks", "close"))
            adj = s.adjusted_close
            assert isinstance(adj, Alpha)


class TestAlphaSessionEager:
    """Tests for eager mode."""

    def test_eager_prefetch(self, client: QuantDLClient) -> None:
        """Test eager mode prefetches fields."""
        with AlphaSession(
            client, ["AAPL"], "2024-01-02", "2024-01-10",
            eager=True, fields=["close", "volume"]
        ) as s:
            # Fields should already be cached
            assert "close" in s._cache
            assert "volume" in s._cache


class TestAlphaSessionExceptions:
    """Tests for session exceptions."""

    def test_field_not_found(self, client: QuantDLClient) -> None:
        """Test FieldNotFoundError on unknown field."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            with pytest.raises(FieldNotFoundError) as exc:
                _ = s.nonexistent_field
            assert "nonexistent_field" in str(exc.value)

    def test_session_not_active_getattr(self, client: QuantDLClient) -> None:
        """Test SessionNotActiveError on attribute access outside context."""
        session = AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10")
        with pytest.raises(SessionNotActiveError):
            _ = session.close

    def test_session_not_active_fetch(self, client: QuantDLClient) -> None:
        """Test SessionNotActiveError on fetch outside context."""
        session = AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10")
        with pytest.raises(SessionNotActiveError):
            session.fetch("close")

    def test_session_not_active_eval(self, client: QuantDLClient) -> None:
        """Test SessionNotActiveError on eval outside context."""
        session = AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10")
        with pytest.raises(SessionNotActiveError):
            session.eval("close + 1")


class TestAlphaSessionOperators:
    """Tests for using Alpha with operators."""

    def test_alpha_arithmetic(self, client: QuantDLClient) -> None:
        """Test Alpha arithmetic operations."""
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            close = s.close
            result = close + 10
            assert isinstance(result, Alpha)

    def test_alpha_with_ops(self, client: QuantDLClient) -> None:
        """Test Alpha.data with operators."""
        import quantdl.operators as ops
        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            close = s.close
            delta = ops.ts_delta(close.data, 1)
            assert isinstance(delta, pl.DataFrame)


class TestAlphaSessionChunking:
    """Tests for chunking functionality."""

    def test_chunking_single_chunk(self, client: QuantDLClient) -> None:
        """Test chunk_size > len(symbols) works."""
        with AlphaSession(
            client, ["AAPL"], "2024-01-02", "2024-01-10",
            chunk_size=500
        ) as s:
            close = s.close
            assert isinstance(close, Alpha)
            assert "AAPL" in close.data.columns

    def test_no_chunking(self, client: QuantDLClient) -> None:
        """Test chunk_size=None (no chunking)."""
        with AlphaSession(
            client, ["AAPL"], "2024-01-02", "2024-01-10",
            chunk_size=None
        ) as s:
            close = s.close
            assert isinstance(close, Alpha)


class TestAlphaSessionThreadSafety:
    """Tests for thread safety."""

    def test_concurrent_same_field(self, client: QuantDLClient) -> None:
        """Test concurrent access to same field."""
        results: list[Alpha] = []
        errors: list[Exception] = []

        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            def worker() -> None:
                try:
                    results.append(s.close)
                except Exception as e:
                    errors.append(e)

            threads = [threading.Thread(target=worker) for _ in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert len(errors) == 0
        assert len(results) == 5
        # All should return same cached object
        assert all(r is results[0] for r in results)

    def test_concurrent_different_fields(self, client: QuantDLClient) -> None:
        """Test concurrent access to different fields."""
        results: dict[str, Alpha] = {}
        errors: list[Exception] = []
        lock = threading.Lock()

        with AlphaSession(client, ["AAPL"], "2024-01-02", "2024-01-10") as s:
            def fetch_close() -> None:
                try:
                    r = s.close
                    with lock:
                        results["close"] = r
                except Exception as e:
                    errors.append(e)

            def fetch_volume() -> None:
                try:
                    r = s.volume
                    with lock:
                        results["volume"] = r
                except Exception as e:
                    errors.append(e)

            def fetch_open() -> None:
                try:
                    r = s.open
                    with lock:
                        results["open"] = r
                except Exception as e:
                    errors.append(e)

            threads = [
                threading.Thread(target=fetch_close),
                threading.Thread(target=fetch_volume),
                threading.Thread(target=fetch_open),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        assert len(errors) == 0
        assert isinstance(results.get("close"), Alpha)
        assert isinstance(results.get("volume"), Alpha)
        assert isinstance(results.get("open"), Alpha)


class TestDataSpec:
    """Tests for DataSpec type."""

    def test_dataspec_creation(self) -> None:
        """Test DataSpec creation."""
        spec = DataSpec("ticks", "close")
        assert spec.source == "ticks"
        assert spec.field == "close"

    def test_dataspec_immutable(self) -> None:
        """Test DataSpec is immutable (frozen)."""
        spec = DataSpec("ticks", "close")
        with pytest.raises(dataclasses.FrozenInstanceError):
            spec.field = "open"  # type: ignore[misc]

    def test_dataspec_hashable(self) -> None:
        """Test DataSpec is hashable."""
        spec1 = DataSpec("ticks", "close")
        spec2 = DataSpec("ticks", "close")
        assert hash(spec1) == hash(spec2)
        assert spec1 == spec2
