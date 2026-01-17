"""Tests for S3StorageBackend."""

from pathlib import Path

import polars as pl
import pytest

from quantdl.exceptions import S3Error
from quantdl.storage.s3 import S3StorageBackend


@pytest.fixture
def local_storage(test_data_dir: Path) -> S3StorageBackend:
    """Create storage backend with local test data."""
    return S3StorageBackend(bucket="us-equity-datalake", local_path=test_data_dir)


class TestS3StorageBasics:
    """Basic S3 storage tests."""

    def test_local_mode_read(self, local_storage: S3StorageBackend) -> None:
        """Test reading parquet in local mode."""
        df = local_storage.read_parquet("data/master/security_master.parquet")
        assert len(df) > 0
        assert "security_id" in df.columns

    def test_local_mode_scan(self, local_storage: S3StorageBackend) -> None:
        """Test scanning parquet in local mode."""
        lf = local_storage.scan_parquet("data/master/security_master.parquet")
        df = lf.collect()
        assert len(df) > 0

    def test_exists_local_true(self, local_storage: S3StorageBackend) -> None:
        """Test exists returns True for existing local file."""
        result = local_storage.exists("data/master/security_master.parquet")
        assert result is True

    def test_exists_local_false(self, local_storage: S3StorageBackend) -> None:
        """Test exists returns False for non-existing local file."""
        result = local_storage.exists("data/master/nonexistent.parquet")
        assert result is False


class TestS3Credentials:
    """Tests for S3 credential handling."""

    def test_s3_explicit_credentials(self) -> None:
        """Test S3StorageBackend accepts explicit credentials."""
        storage = S3StorageBackend(
            bucket="test-bucket",
            aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
            aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            aws_region="us-west-2",
        )
        # Verify credentials stored in storage options
        assert storage._storage_options["aws_access_key_id"] == "AKIAIOSFODNN7EXAMPLE"
        assert storage._storage_options["aws_secret_access_key"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        assert storage._storage_options["aws_region"] == "us-west-2"

    def test_s3_no_credentials_local_mode(self) -> None:
        """Test S3StorageBackend works without credentials in local mode."""
        storage = S3StorageBackend(bucket="test", local_path="/tmp/test")
        # Local mode should not have storage options
        assert len(storage._storage_options) == 0


class TestS3Errors:
    """Tests for S3 error handling."""

    def test_read_parquet_error_wrapping(self, local_storage: S3StorageBackend) -> None:
        """Test read_parquet wraps errors in S3Error."""
        with pytest.raises(S3Error) as exc_info:
            local_storage.read_parquet("nonexistent/path.parquet")
        assert "nonexistent/path.parquet" in str(exc_info.value)

    def test_read_parquet_error_has_cause(self, local_storage: S3StorageBackend) -> None:
        """Test S3Error includes original cause."""
        with pytest.raises(S3Error) as exc_info:
            local_storage.read_parquet("nonexistent/path.parquet")
        assert exc_info.value.cause is not None


class TestS3PathResolution:
    """Tests for path resolution."""

    def test_resolve_path_local(self, test_data_dir: Path) -> None:
        """Test path resolution in local mode."""
        storage = S3StorageBackend(bucket="test", local_path=test_data_dir)
        resolved = storage._resolve_path("/data/test.parquet")
        assert str(test_data_dir) in resolved
        assert "data/test.parquet" in resolved.replace("\\", "/")

    def test_resolve_path_s3(self) -> None:
        """Test path resolution in S3 mode."""
        storage = S3StorageBackend(bucket="my-bucket")
        resolved = storage._resolve_path("/data/test.parquet")
        assert resolved == "s3://my-bucket/data/test.parquet"

    def test_resolve_path_strips_leading_slash(self) -> None:
        """Test path resolution strips leading slash."""
        storage = S3StorageBackend(bucket="my-bucket")
        resolved1 = storage._resolve_path("/data/test.parquet")
        resolved2 = storage._resolve_path("data/test.parquet")
        assert resolved1 == resolved2


class TestS3ColumnSelection:
    """Tests for column selection."""

    def test_scan_parquet_with_columns(self, local_storage: S3StorageBackend) -> None:
        """Test scanning with column selection."""
        lf = local_storage.scan_parquet(
            "data/master/security_master.parquet",
            columns=["security_id", "symbol"],
        )
        df = lf.collect()
        assert df.columns == ["security_id", "symbol"]

    def test_read_parquet_with_columns(self, local_storage: S3StorageBackend) -> None:
        """Test reading with column selection."""
        df = local_storage.read_parquet(
            "data/master/security_master.parquet",
            columns=["security_id", "symbol"],
        )
        assert df.columns == ["security_id", "symbol"]

    def test_read_parquet_with_filters(self, local_storage: S3StorageBackend) -> None:
        """Test reading with filters."""
        df = local_storage.read_parquet(
            "data/master/security_master.parquet",
            filters=[pl.col("symbol") == "AAPL"],
        )
        assert len(df) == 1
        assert df["symbol"][0] == "AAPL"
