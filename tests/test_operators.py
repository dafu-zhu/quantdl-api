"""Tests for alpha operators."""

import math
from datetime import date

import polars as pl
import pytest

from quantdl.operators import (
    days_from_last_change,
    hump,
    kth_element,
    last_diff_value,
    normalize,
    quantile,
    rank,
    scale,
    ts_arg_max,
    ts_arg_min,
    ts_av_diff,
    ts_backfill,
    ts_corr,
    ts_count_nans,
    ts_covariance,
    ts_decay_linear,
    ts_delay,
    ts_delta,
    ts_max,
    ts_mean,
    ts_min,
    ts_product,
    ts_quantile,
    ts_rank,
    ts_regression,
    ts_scale,
    ts_std,
    ts_step,
    ts_sum,
    ts_zscore,
    winsorize,
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

    def test_ts_product(self, wide_df: pl.DataFrame) -> None:
        """Test rolling product."""
        result = ts_product(wide_df, 3)
        assert result.columns == wide_df.columns
        # Product of 100, 102, 101
        expected = 100.0 * 102.0 * 101.0
        assert abs(result["AAPL"][2] - expected) < 0.01

    def test_ts_count_nans(self) -> None:
        """Test counting nulls in window."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 5), eager=True),
            "AAPL": [100.0, None, 101.0, None, 105.0],
        })
        result = ts_count_nans(df, 3)
        # At idx 2: window [100, None, 101] has 1 null
        assert result["AAPL"][2] == 1
        # At idx 3: window [None, 101, None] has 2 nulls
        assert result["AAPL"][3] == 2

    def test_ts_zscore(self, wide_df: pl.DataFrame) -> None:
        """Test rolling z-score."""
        result = ts_zscore(wide_df, 3)
        assert result.columns == wide_df.columns
        # Z-score exists for idx >= 2
        assert result["AAPL"][2] is not None
        # Z-score should be finite
        assert not math.isnan(result["AAPL"][2])

    def test_ts_scale(self, wide_df: pl.DataFrame) -> None:
        """Test rolling min-max scale."""
        result = ts_scale(wide_df, 3)
        assert result.columns == wide_df.columns
        # Values should be in [0, 1] range
        for i in range(2, len(result)):
            val = result["AAPL"][i]
            if val is not None:
                assert 0.0 <= val <= 1.0

    def test_ts_av_diff(self, wide_df: pl.DataFrame) -> None:
        """Test difference from rolling mean."""
        result = ts_av_diff(wide_df, 3)
        # At idx 2: value=101, mean=(100+102+101)/3=101, diff=0
        assert abs(result["AAPL"][2]) < 0.01

    def test_ts_step(self, wide_df: pl.DataFrame) -> None:
        """Test row counter."""
        result = ts_step(wide_df)
        assert result["AAPL"][0] == 1
        assert result["AAPL"][4] == 5
        assert result["AAPL"][9] == 10

    def test_ts_arg_max(self, wide_df: pl.DataFrame) -> None:
        """Test index of max in window."""
        result = ts_arg_max(wide_df, 3)
        # At idx 2: window [100, 102, 101], max is 102 at idx 1
        assert result["AAPL"][2] == 1.0

    def test_ts_arg_min(self, wide_df: pl.DataFrame) -> None:
        """Test index of min in window."""
        result = ts_arg_min(wide_df, 3)
        # At idx 2: window [100, 102, 101], min is 100 at idx 0
        assert result["AAPL"][2] == 0.0

    def test_ts_backfill(self) -> None:
        """Test forward fill with limit."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 5), eager=True),
            "AAPL": [100.0, None, None, None, 105.0],
        })
        result = ts_backfill(df, 2)
        # Should fill first 2 nulls
        assert result["AAPL"][1] == 100.0
        assert result["AAPL"][2] == 100.0
        # Third null exceeds limit, stays null
        assert result["AAPL"][3] is None

    def test_kth_element(self, wide_df: pl.DataFrame) -> None:
        """Test k-th element lookback."""
        result = kth_element(wide_df, 5, 2)
        # k=2 means 2 periods ago
        assert result["AAPL"][2] == 100.0
        assert result["AAPL"][3] == 102.0

    def test_last_diff_value(self) -> None:
        """Test finding last different value."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 5), eager=True),
            "AAPL": [100.0, 100.0, 102.0, 102.0, 102.0],
        })
        result = last_diff_value(df, 3)
        # At idx 4: window [102, 102, 102], current=102, last diff in window is 100 at idx 2
        # But idx 2 is not in window [2,3,4], so None
        # Actually at idx 3: window [100, 102, 102], current=102, last diff=100
        assert result["AAPL"][3] == 100.0

    def test_days_from_last_change(self) -> None:
        """Test days since value changed."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 5), eager=True),
            "AAPL": [100.0, 100.0, 102.0, 102.0, 102.0],
        })
        result = days_from_last_change(df)
        assert result["AAPL"][0] == 0  # First row
        assert result["AAPL"][1] == 1  # Same as prev
        assert result["AAPL"][2] == 0  # Changed
        assert result["AAPL"][3] == 1  # Same as prev
        assert result["AAPL"][4] == 2  # 2 days since change

    def test_hump(self) -> None:
        """Test hump limiting change magnitude."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 3), eager=True),
            "A": [100.0, 200.0, 150.0],
            "B": [50.0, 50.0, 50.0],
        })
        result = hump(df, hump=0.1)
        # Row 1: sum(|values|) = 200+50=250, limit=25
        # A change = 100, capped at prev + 25 = 125
        assert result["A"][1] == 125.0

    def test_ts_decay_linear(self, wide_df: pl.DataFrame) -> None:
        """Test linear decay weighted average."""
        result = ts_decay_linear(wide_df, 3)
        # Weights [1, 2, 3], sum=6
        # At idx 2: (100*1 + 102*2 + 101*3) / 6
        expected = (100 * 1 + 102 * 2 + 101 * 3) / 6
        assert abs(result["AAPL"][2] - expected) < 0.01

    def test_ts_rank(self, wide_df: pl.DataFrame) -> None:
        """Test rank of current value in window."""
        result = ts_rank(wide_df, 3)
        # At idx 2: window [100, 102, 101], current=101
        # Sorted: [100, 101, 102], idx=1, rank=1/2=0.5
        assert abs(result["AAPL"][2] - 0.5) < 0.01

    def test_ts_corr(self, wide_df: pl.DataFrame) -> None:
        """Test rolling correlation."""
        # Correlate with itself should give 1.0
        result = ts_corr(wide_df, wide_df, 3)
        assert abs(result["AAPL"][2] - 1.0) < 0.01

    def test_ts_covariance(self, wide_df: pl.DataFrame) -> None:
        """Test rolling covariance."""
        result = ts_covariance(wide_df, wide_df, 3)
        # Cov with self = variance
        assert result["AAPL"][2] is not None
        assert result["AAPL"][2] > 0

    def test_ts_quantile_gaussian(self, wide_df: pl.DataFrame) -> None:
        """Test rolling quantile with gaussian transform."""
        result = ts_quantile(wide_df, 3, driver="gaussian")
        assert result.columns == wide_df.columns
        # Should produce finite values
        for i in range(2, len(result)):
            val = result["AAPL"][i]
            if val is not None:
                assert not math.isinf(val)

    def test_ts_regression_residual(self, wide_df: pl.DataFrame) -> None:
        """Test regression residual (rettype=0)."""
        result = ts_regression(wide_df, wide_df, 3, rettype=0)
        # Regressing on itself gives perfect fit, residual=0
        for i in range(2, len(result)):
            val = result["AAPL"][i]
            if val is not None:
                assert abs(val) < 0.01

    def test_ts_regression_beta(self, wide_df: pl.DataFrame) -> None:
        """Test regression beta (rettype=1)."""
        result = ts_regression(wide_df, wide_df, 3, rettype=1)
        # Regressing on itself, beta=1
        for i in range(2, len(result)):
            val = result["AAPL"][i]
            if val is not None:
                assert abs(val - 1.0) < 0.01

    def test_ts_regression_rsquared(self, wide_df: pl.DataFrame) -> None:
        """Test regression r-squared (rettype=5)."""
        result = ts_regression(wide_df, wide_df, 3, rettype=5)
        # Regressing on itself, r-squared=1
        for i in range(2, len(result)):
            val = result["AAPL"][i]
            if val is not None:
                assert abs(val - 1.0) < 0.01


class TestCrossSectionalOperators:
    """Cross-sectional operator tests."""

    def test_rank(self, wide_df: pl.DataFrame) -> None:
        """Test cross-sectional rank returns [0,1] floats."""
        result = rank(wide_df, rate=0)  # Precise ranking

        assert result.columns == wide_df.columns

        # At each date, AAPL < GOOGL < MSFT, so ranks should be 0.0, 0.5, 1.0
        # Check first row
        assert result["AAPL"][0] == 0.0  # Smallest
        assert result["GOOGL"][0] == 0.5
        assert result["MSFT"][0] == 1.0  # Largest

    def test_rank_approximate(self, wide_df: pl.DataFrame) -> None:
        """Test approximate ranking with rate>0."""
        result = rank(wide_df, rate=2)  # Bucket-based ranking

        assert result.columns == wide_df.columns
        # Values should be in [0, 1]
        for col in ["AAPL", "MSFT", "GOOGL"]:
            for val in result[col]:
                if val is not None:
                    assert 0.0 <= val <= 1.0

    def test_zscore(self, wide_df: pl.DataFrame) -> None:
        """Test cross-sectional z-score."""
        result = zscore(wide_df)

        assert result.columns == wide_df.columns

        # Z-scores should sum to ~0 for each row
        for i in range(len(result)):
            row_sum = result["AAPL"][i] + result["MSFT"][i] + result["GOOGL"][i]
            assert abs(row_sum) < 0.01

    def test_normalize(self, wide_df: pl.DataFrame) -> None:
        """Test cross-sectional normalize (demean)."""
        result = normalize(wide_df)

        # Normalized values should sum to ~0 for each row
        for i in range(len(result)):
            row_sum = result["AAPL"][i] + result["MSFT"][i] + result["GOOGL"][i]
            assert abs(row_sum) < 0.01

    def test_normalize_with_std(self, wide_df: pl.DataFrame) -> None:
        """Test normalize with std division."""
        result = normalize(wide_df, useStd=True)

        # Should be similar to zscore
        for i in range(len(result)):
            row_sum = result["AAPL"][i] + result["MSFT"][i] + result["GOOGL"][i]
            assert abs(row_sum) < 0.01

    def test_normalize_with_limit(self, wide_df: pl.DataFrame) -> None:
        """Test normalize with clipping."""
        result = normalize(wide_df, useStd=True, limit=0.5)

        # Values should be clipped to [-0.5, 0.5]
        for col in ["AAPL", "MSFT", "GOOGL"]:
            for val in result[col]:
                if val is not None:
                    assert -0.5 <= val <= 0.5

    def test_scale(self, wide_df: pl.DataFrame) -> None:
        """Test scaling to target."""
        result = scale(wide_df, scale=1.0)

        # Sum of absolute values should be ~1.0 for each row
        for i in range(len(result)):
            abs_sum = abs(result["AAPL"][i]) + abs(result["MSFT"][i]) + abs(result["GOOGL"][i])
            assert abs(abs_sum - 1.0) < 0.01

    def test_scale_custom_booksize(self, wide_df: pl.DataFrame) -> None:
        """Test scaling to custom book size."""
        result = scale(wide_df, scale=4.0)

        # Sum of absolute values should be ~4.0 for each row
        for i in range(len(result)):
            abs_sum = abs(result["AAPL"][i]) + abs(result["MSFT"][i]) + abs(result["GOOGL"][i])
            assert abs(abs_sum - 4.0) < 0.01

    def test_scale_longscale_shortscale(self) -> None:
        """Test asymmetric long/short scaling."""
        # Create data with both positive and negative values
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 3), eager=True),
            "A": [10.0, -5.0, 20.0],
            "B": [-20.0, 15.0, -10.0],
            "C": [5.0, -10.0, 30.0],
        })

        result = scale(df, longscale=4.0, shortscale=3.0)

        # Check row 0: longs = [10, 5] = 15, shorts = [|-20|] = 20
        # After scaling: longs sum to 4, shorts sum to 3
        row0_long_sum = max(0, result["A"][0]) + max(0, result["C"][0])
        row0_short_sum = abs(min(0, result["B"][0]))
        assert abs(row0_long_sum - 4.0) < 0.01
        assert abs(row0_short_sum - 3.0) < 0.01

        # Check row 1: longs = [15] = 15, shorts = [|-5|, |-10|] = 15
        row1_long_sum = max(0, result["B"][1])
        row1_short_sum = abs(min(0, result["A"][1])) + abs(min(0, result["C"][1]))
        assert abs(row1_long_sum - 4.0) < 0.01
        assert abs(row1_short_sum - 3.0) < 0.01

    def test_scale_only_longscale(self) -> None:
        """Test scaling only long positions."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 2), eager=True),
            "A": [10.0, -5.0],
            "B": [-20.0, 15.0],
            "C": [5.0, -10.0],
        })

        result = scale(df, longscale=2.0, shortscale=0.0)

        # Row 0: longs = [10, 5] = 15, should sum to 2
        row0_long_sum = max(0, result["A"][0]) + max(0, result["C"][0])
        assert abs(row0_long_sum - 2.0) < 0.01
        # Shorts should be 0 (shortscale=0)
        assert result["B"][0] == 0.0

    def test_scale_only_shortscale(self) -> None:
        """Test scaling only short positions."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 2), eager=True),
            "A": [10.0, -5.0],
            "B": [-20.0, 15.0],
            "C": [5.0, -10.0],
        })

        result = scale(df, longscale=0.0, shortscale=3.0)

        # Row 0: shorts = [|-20|] = 20, should sum to 3
        row0_short_sum = abs(min(0, result["B"][0]))
        assert abs(row0_short_sum - 3.0) < 0.01
        # Longs should be 0 (longscale=0)
        assert result["A"][0] == 0.0
        assert result["C"][0] == 0.0

    def test_quantile_gaussian(self, wide_df: pl.DataFrame) -> None:
        """Test quantile transformation with gaussian driver."""
        result = quantile(wide_df, driver="gaussian")

        assert result.columns == wide_df.columns
        # Output should be finite for all non-null values
        for col in ["AAPL", "MSFT", "GOOGL"]:
            for val in result[col]:
                if val is not None:
                    assert not math.isnan(val)

    def test_quantile_uniform(self, wide_df: pl.DataFrame) -> None:
        """Test quantile transformation with uniform driver."""
        result = quantile(wide_df, driver="uniform", sigma=2.0)

        assert result.columns == wide_df.columns
        # Uniform output should be in [-sigma, sigma]
        for col in ["AAPL", "MSFT", "GOOGL"]:
            for val in result[col]:
                if val is not None:
                    assert -2.0 <= val <= 2.0

    def test_winsorize(self, wide_df: pl.DataFrame) -> None:
        """Test winsorization."""
        result = winsorize(wide_df, std=1.0)

        assert result.columns == wide_df.columns
        # Winsorized values should not have extreme outliers
        # At least check that values exist
        assert len(result) == len(wide_df)

    def test_winsorize_with_outliers(self) -> None:
        """Test winsorization clips outliers."""
        df = pl.DataFrame({
            "timestamp": pl.date_range(date(2024, 1, 1), date(2024, 1, 1), eager=True),
            "A": [1.0],
            "B": [2.0],
            "C": [100.0],  # Outlier
        })

        result = winsorize(df, std=1.0)
        # The outlier should be clipped
        assert result["C"][0] < 100.0


class TestOperatorComposition:
    """Test composing operators."""

    def test_ts_mean_then_rank(self, wide_df: pl.DataFrame) -> None:
        """Test composing time-series and cross-sectional operators."""
        ma = ts_mean(wide_df, 3)
        ranked = rank(ma)

        assert ranked.columns == wide_df.columns
        # First 2 rows have nulls from rolling mean, rank returns NaN
        assert ranked["AAPL"][0] is None or math.isnan(ranked["AAPL"][0])
        assert ranked["AAPL"][1] is None or math.isnan(ranked["AAPL"][1])

    def test_normalize_then_scale(self, wide_df: pl.DataFrame) -> None:
        """Test composing cross-sectional operators."""
        normalized = normalize(wide_df)
        scaled = scale(normalized, scale=1.0)

        # Should still sum to ~0 (normalize preserved)
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
