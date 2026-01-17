"""Tests for alpha operators."""

from datetime import date

import polars as pl
import pytest

from quantdl.operators import (
    demean,
    rank,
    scale,
    ts_delay,
    ts_delta,
    ts_max,
    ts_mean,
    ts_min,
    ts_std,
    ts_sum,
    zscore,
)


@pytest.fixture
def wide_df() -> pl.DataFrame:
    """Create sample wide DataFrame."""
    return pl.DataFrame({
        "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 10), eager=True),
        "AAPL": [100.0, 102.0, 101.0, 103.0, 105.0, 104.0, 106.0, 108.0, 107.0, 110.0],
        "MSFT": [200.0, 202.0, 201.0, 203.0, 205.0, 204.0, 206.0, 208.0, 207.0, 210.0],
        "GOOGL": [150.0, 152.0, 151.0, 153.0, 155.0, 154.0, 156.0, 158.0, 157.0, 160.0],
    })


class TestTimeSeriesOperators:
    """Time-series operator tests."""

    def test_ts_mean(self, wide_df: pl.DataFrame) -> None:
        """Test rolling mean."""
        result = ts_mean(wide_df, 3)

        assert result.columns == wide_df.columns
        assert len(result) == len(wide_df)

        # First 2 values should be null
        assert result["AAPL"][0] is None
        assert result["AAPL"][1] is None

        # Third value should be mean of first 3
        expected = (100.0 + 102.0 + 101.0) / 3
        assert abs(result["AAPL"][2] - expected) < 0.01

    def test_ts_sum(self, wide_df: pl.DataFrame) -> None:
        """Test rolling sum."""
        result = ts_sum(wide_df, 3)

        expected = 100.0 + 102.0 + 101.0
        assert abs(result["AAPL"][2] - expected) < 0.01

    def test_ts_std(self, wide_df: pl.DataFrame) -> None:
        """Test rolling standard deviation."""
        result = ts_std(wide_df, 3)

        assert result.columns == wide_df.columns
        # Std should be positive
        assert result["AAPL"][2] is not None
        assert result["AAPL"][2] > 0

    def test_ts_min(self, wide_df: pl.DataFrame) -> None:
        """Test rolling minimum."""
        result = ts_min(wide_df, 3)

        # Min of 100, 102, 101 is 100
        assert result["AAPL"][2] == 100.0

    def test_ts_max(self, wide_df: pl.DataFrame) -> None:
        """Test rolling maximum."""
        result = ts_max(wide_df, 3)

        # Max of 100, 102, 101 is 102
        assert result["AAPL"][2] == 102.0

    def test_ts_delta(self, wide_df: pl.DataFrame) -> None:
        """Test difference."""
        result = ts_delta(wide_df, 1)

        # Second value - first value = 102 - 100 = 2
        assert result["AAPL"][1] == 2.0

    def test_ts_delay(self, wide_df: pl.DataFrame) -> None:
        """Test lag."""
        result = ts_delay(wide_df, 1)

        # First value should be null
        assert result["AAPL"][0] is None
        # Second value should be first original value
        assert result["AAPL"][1] == 100.0


class TestCrossSectionalOperators:
    """Cross-sectional operator tests."""

    def test_rank(self, wide_df: pl.DataFrame) -> None:
        """Test cross-sectional rank."""
        result = rank(wide_df)

        assert result.columns == wide_df.columns

        # At each date, AAPL < GOOGL < MSFT, so ranks should be 1, 2, 3
        # Check first row
        assert result["AAPL"][0] == 1  # Smallest
        assert result["GOOGL"][0] == 2
        assert result["MSFT"][0] == 3  # Largest

    def test_rank_descending(self, wide_df: pl.DataFrame) -> None:
        """Test descending rank."""
        result = rank(wide_df, ascending=False)

        # Largest gets rank 1
        assert result["MSFT"][0] == 1
        assert result["AAPL"][0] == 3

    def test_zscore(self, wide_df: pl.DataFrame) -> None:
        """Test cross-sectional z-score."""
        result = zscore(wide_df)

        assert result.columns == wide_df.columns

        # Z-scores should sum to ~0 for each row
        for i in range(len(result)):
            row_sum = result["AAPL"][i] + result["MSFT"][i] + result["GOOGL"][i]
            assert abs(row_sum) < 0.01

    def test_demean(self, wide_df: pl.DataFrame) -> None:
        """Test cross-sectional demean."""
        result = demean(wide_df)

        # Demeaned values should sum to ~0 for each row
        for i in range(len(result)):
            row_sum = result["AAPL"][i] + result["MSFT"][i] + result["GOOGL"][i]
            assert abs(row_sum) < 0.01

    def test_scale(self, wide_df: pl.DataFrame) -> None:
        """Test scaling to target."""
        result = scale(wide_df, target=1.0)

        # Sum of absolute values should be ~1.0 for each row
        for i in range(len(result)):
            abs_sum = abs(result["AAPL"][i]) + abs(result["MSFT"][i]) + abs(result["GOOGL"][i])
            assert abs(abs_sum - 1.0) < 0.01


class TestOperatorComposition:
    """Test composing operators."""

    def test_ts_mean_then_rank(self, wide_df: pl.DataFrame) -> None:
        """Test composing time-series and cross-sectional operators."""
        ma = ts_mean(wide_df, 3)
        ranked = rank(ma)

        assert ranked.columns == wide_df.columns
        # First 2 rows have nulls from rolling mean
        assert ranked["AAPL"][0] is None
        assert ranked["AAPL"][1] is None

    def test_demean_then_scale(self, wide_df: pl.DataFrame) -> None:
        """Test composing cross-sectional operators."""
        demeaned = demean(wide_df)
        scaled = scale(demeaned, target=1.0)

        # Should still sum to ~0 (demean preserved)
        # But absolute sum should be ~1 (scale)
        for i in range(len(scaled)):
            row_sum = scaled["AAPL"][i] + scaled["MSFT"][i] + scaled["GOOGL"][i]
            assert abs(row_sum) < 0.01


class TestEdgeCases:
    """Edge case tests."""

    def test_single_column(self) -> None:
        """Test operators with single symbol column."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 5), eager=True),
            "AAPL": [100.0, 102.0, 101.0, 103.0, 105.0],
        })

        result = ts_mean(df, 3)
        assert result.columns == df.columns

    def test_with_nulls(self) -> None:
        """Test operators handle nulls correctly."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 5), eager=True),
            "AAPL": [100.0, None, 101.0, 103.0, 105.0],
            "MSFT": [200.0, 202.0, None, 203.0, 205.0],
        })

        result = ts_mean(df, 3)
        # Should not raise, nulls propagate
        assert result is not None
