"""Alpha operators for wide table transformations.

All operators work on wide DataFrames where:
- First column is the date/timestamp
- Remaining columns are symbol values

Example:
    ```python
    from quantdl.operators import ts_mean, rank, zscore

    # Apply 20-day moving average
    ma = ts_mean(prices, 20)

    # Cross-sectional rank
    ranked = rank(ma)

    # Z-score within each date
    standardized = zscore(ma)
    ```
"""

from quantdl.operators.cross_sectional import (
    normalize,
    quantile,
    rank,
    scale,
    winsorize,
    zscore,
)
from quantdl.operators.time_series import (
    days_from_last_change,
    hump,
    kth_element,
    last_diff_value,
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
)

__all__ = [
    # Time-series operators (basic)
    "ts_mean",
    "ts_sum",
    "ts_std",
    "ts_min",
    "ts_max",
    "ts_delta",
    "ts_delay",
    # Time-series operators (rolling)
    "ts_product",
    "ts_count_nans",
    "ts_zscore",
    "ts_scale",
    "ts_av_diff",
    "ts_step",
    # Time-series operators (arg)
    "ts_arg_max",
    "ts_arg_min",
    # Time-series operators (lookback)
    "ts_backfill",
    "kth_element",
    "last_diff_value",
    "days_from_last_change",
    # Time-series operators (stateful)
    "hump",
    "ts_decay_linear",
    "ts_rank",
    # Time-series operators (two-variable)
    "ts_corr",
    "ts_covariance",
    "ts_quantile",
    "ts_regression",
    # Cross-sectional operators
    "rank",
    "zscore",
    "normalize",
    "scale",
    "quantile",
    "winsorize",
]
