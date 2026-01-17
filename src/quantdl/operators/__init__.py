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

from quantdl.operators.crosssectional import demean, rank, scale, zscore
from quantdl.operators.timeseries import (
    ts_delay,
    ts_delta,
    ts_max,
    ts_mean,
    ts_min,
    ts_std,
    ts_sum,
)

__all__ = [
    # Time-series operators
    "ts_mean",
    "ts_sum",
    "ts_std",
    "ts_min",
    "ts_max",
    "ts_delta",
    "ts_delay",
    # Cross-sectional operators
    "rank",
    "zscore",
    "demean",
    "scale",
]
