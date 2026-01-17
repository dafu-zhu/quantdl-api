"""Time-series operators for wide tables.

All operators preserve the wide table structure:
- First column (date) is unchanged
- Operations applied column-wise to symbol columns
"""

import polars as pl


def _get_value_cols(df: pl.DataFrame) -> list[str]:
    """Get value columns (all except first which is date)."""
    return df.columns[1:]


def ts_mean(x: pl.DataFrame, d: int) -> pl.DataFrame:
    """Rolling mean over d periods.

    Args:
        x: Wide DataFrame with date + symbol columns
        d: Window size in periods

    Returns:
        Wide DataFrame with rolling mean values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).rolling_mean(d).alias(c) for c in value_cols],
    )


def ts_sum(x: pl.DataFrame, d: int) -> pl.DataFrame:
    """Rolling sum over d periods.

    Args:
        x: Wide DataFrame with date + symbol columns
        d: Window size in periods

    Returns:
        Wide DataFrame with rolling sum values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).rolling_sum(d).alias(c) for c in value_cols],
    )


def ts_std(x: pl.DataFrame, d: int) -> pl.DataFrame:
    """Rolling standard deviation over d periods.

    Args:
        x: Wide DataFrame with date + symbol columns
        d: Window size in periods

    Returns:
        Wide DataFrame with rolling std values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).rolling_std(d).alias(c) for c in value_cols],
    )


def ts_min(x: pl.DataFrame, d: int) -> pl.DataFrame:
    """Rolling minimum over d periods.

    Args:
        x: Wide DataFrame with date + symbol columns
        d: Window size in periods

    Returns:
        Wide DataFrame with rolling min values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).rolling_min(d).alias(c) for c in value_cols],
    )


def ts_max(x: pl.DataFrame, d: int) -> pl.DataFrame:
    """Rolling maximum over d periods.

    Args:
        x: Wide DataFrame with date + symbol columns
        d: Window size in periods

    Returns:
        Wide DataFrame with rolling max values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).rolling_max(d).alias(c) for c in value_cols],
    )


def ts_delta(x: pl.DataFrame, d: int = 1) -> pl.DataFrame:
    """Difference from d periods ago: x - ts_delay(x, d).

    Args:
        x: Wide DataFrame with date + symbol columns
        d: Lag periods (default: 1)

    Returns:
        Wide DataFrame with differenced values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).diff(d).alias(c) for c in value_cols],
    )


def ts_delay(x: pl.DataFrame, d: int) -> pl.DataFrame:
    """Lag values by d periods.

    Args:
        x: Wide DataFrame with date + symbol columns
        d: Number of periods to lag

    Returns:
        Wide DataFrame with lagged values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).shift(d).alias(c) for c in value_cols],
    )
