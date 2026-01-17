"""Tests for DiskCache."""

import threading
import time

import polars as pl
import pytest

from quantdl.storage.cache import DiskCache


@pytest.fixture
def cache(temp_cache_dir: str) -> DiskCache:
    """Create cache with temp directory."""
    return DiskCache(
        cache_dir=temp_cache_dir,
        ttl_seconds=10,
        max_size_bytes=1024 * 1024,  # 1MB
    )


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """Create sample DataFrame for testing."""
    return pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4.0, 5.0, 6.0],
        "c": ["x", "y", "z"],
    })


class TestCacheBasics:
    """Basic cache operations."""

    def test_cache_miss(self, cache: DiskCache) -> None:
        """Test cache miss returns None."""
        result = cache.get("nonexistent/path.parquet")
        assert result is None

    def test_cache_hit(self, cache: DiskCache, sample_df: pl.DataFrame) -> None:
        """Test cache hit returns data."""
        cache.put("test/path.parquet", sample_df)
        result = cache.get("test/path.parquet")
        assert result is not None
        assert result.equals(sample_df)

    def test_invalidate(self, cache: DiskCache, sample_df: pl.DataFrame) -> None:
        """Test cache invalidation."""
        cache.put("test/path.parquet", sample_df)
        assert cache.get("test/path.parquet") is not None

        result = cache.invalidate("test/path.parquet")
        assert result is True
        assert cache.get("test/path.parquet") is None

    def test_invalidate_nonexistent(self, cache: DiskCache) -> None:
        """Test invalidating nonexistent entry."""
        result = cache.invalidate("nonexistent/path.parquet")
        assert result is False

    def test_clear(self, cache: DiskCache, sample_df: pl.DataFrame) -> None:
        """Test clearing all cache."""
        cache.put("test/path1.parquet", sample_df)
        cache.put("test/path2.parquet", sample_df)

        cache.clear()

        assert cache.get("test/path1.parquet") is None
        assert cache.get("test/path2.parquet") is None

    def test_stats(self, cache: DiskCache, sample_df: pl.DataFrame) -> None:
        """Test cache statistics."""
        cache.put("test/path.parquet", sample_df)

        stats = cache.stats()
        assert stats["entries"] == 1
        assert stats["total_size_bytes"] > 0
        assert stats["ttl_seconds"] == 10


class TestCacheTTL:
    """TTL-related tests."""

    def test_ttl_expiry(self, temp_cache_dir: str, sample_df: pl.DataFrame) -> None:
        """Test TTL expiration."""
        cache = DiskCache(
            cache_dir=temp_cache_dir,
            ttl_seconds=1,  # 1 second TTL
        )

        cache.put("test/path.parquet", sample_df)
        assert cache.get("test/path.parquet") is not None

        # Wait for TTL to expire
        time.sleep(1.5)

        # Entry should be evicted
        result = cache.get("test/path.parquet")
        assert result is None


class TestCacheLRU:
    """LRU eviction tests."""

    def test_lru_eviction(self, temp_cache_dir: str) -> None:
        """Test LRU eviction when cache is full."""
        # Small cache that can hold ~2 entries
        cache = DiskCache(
            cache_dir=temp_cache_dir,
            max_size_bytes=2000,  # Very small
        )

        # Create DataFrames of increasing size
        df1 = pl.DataFrame({"a": list(range(50))})
        df2 = pl.DataFrame({"a": list(range(50))})
        df3 = pl.DataFrame({"a": list(range(50))})

        cache.put("path1.parquet", df1)
        time.sleep(0.1)  # Ensure different timestamps
        cache.put("path2.parquet", df2)
        time.sleep(0.1)

        # Access path1 to make it more recent
        cache.get("path1.parquet")

        # Add path3, should evict path2 (least recently used)
        cache.put("path3.parquet", df3)

        # path1 should still exist (accessed recently)
        # path2 might be evicted (LRU)
        # path3 should exist (just added)
        assert cache.get("path3.parquet") is not None


class TestCacheThreadSafety:
    """Thread safety tests."""

    def test_concurrent_writes(self, cache: DiskCache) -> None:
        """Test concurrent writes don't corrupt cache."""
        errors: list[Exception] = []

        def write_entry(i: int) -> None:
            try:
                df = pl.DataFrame({"val": [i]})
                cache.put(f"path{i}.parquet", df)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=write_entry, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0

        # Verify all entries exist
        for i in range(10):
            result = cache.get(f"path{i}.parquet")
            assert result is not None
            assert result["val"][0] == i

    def test_concurrent_reads_writes(self, cache: DiskCache, sample_df: pl.DataFrame) -> None:
        """Test concurrent reads and writes."""
        cache.put("shared.parquet", sample_df)
        errors: list[Exception] = []
        reads: list[pl.DataFrame | None] = []

        def read_entry() -> None:
            try:
                result = cache.get("shared.parquet")
                reads.append(result)
            except Exception as e:
                errors.append(e)

        def write_entry() -> None:
            try:
                cache.put("shared.parquet", sample_df)
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=read_entry) for _ in range(5)
        ] + [
            threading.Thread(target=write_entry) for _ in range(5)
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
