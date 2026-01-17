"""Cross-sectional operators for wide tables.

All operators work row-wise across symbols at each date:
- First column (date) is unchanged
- Operations applied across symbol columns within each row
"""

import polars as pl


def _get_value_cols(df: pl.DataFrame) -> list[str]:
    """Get value columns (all except first which is date)."""
    return df.columns[1:]


def rank(x: pl.DataFrame, ascending: bool = True) -> pl.DataFrame:
    """Cross-sectional rank within each row (date).

    Args:
        x: Wide DataFrame with date + symbol columns
        ascending: If True, smallest value gets rank 1

    Returns:
        Wide DataFrame with rank values (1 to N)
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    # For cross-sectional rank, we need to compute rank across columns for each row
    # Convert to long format, rank, then pivot back
    long = x.unpivot(
        index=date_col,
        on=value_cols,
        variable_name="symbol",
        value_name="value",
    )

    # Rank within each date
    ranked = long.with_columns(
        pl.col("value")
        .rank(method="ordinal", descending=not ascending)
        .over(date_col)
        .alias("value")
    )

    # Pivot back to wide
    wide = ranked.pivot(values="value", index=date_col, on="symbol")

    # Ensure column order matches input
    return wide.select([date_col, *value_cols])


def zscore(x: pl.DataFrame) -> pl.DataFrame:
    """Cross-sectional z-score within each row (date).

    Computes (x - mean) / std across symbols for each date.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with z-scored values
    """
    value_cols = _get_value_cols(x)

    # Compute row-wise mean and std
    row_mean = pl.mean_horizontal(*[pl.col(c) for c in value_cols])
    row_std = pl.concat_list([pl.col(c) for c in value_cols]).list.eval(
        pl.element().std()
    ).list.first()

    return x.with_columns([
        ((pl.col(c) - row_mean) / row_std).alias(c)
        for c in value_cols
    ])


def demean(x: pl.DataFrame) -> pl.DataFrame:
    """Cross-sectional demean within each row (date).

    Subtracts the row mean from each value.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with demeaned values
    """
    value_cols = _get_value_cols(x)

    row_mean = pl.mean_horizontal(*[pl.col(c) for c in value_cols])

    return x.with_columns([
        (pl.col(c) - row_mean).alias(c)
        for c in value_cols
    ])


def scale(
    x: pl.DataFrame,
    scale: float = 1.0,
    longscale: float = 0.0,
    shortscale: float = 0.0,
) -> pl.DataFrame:
    """Scale values so that sum of absolute values equals target book size.

    Scales the input to the book size. The default scales so that sum(abs(x))
    equals 1. Use `scale` parameter to set a different book size.

    For separate long/short scaling, use `longscale` and `shortscale` parameters
    to scale positive and negative positions independently.

    This operator may help reduce outliers.

    Args:
        x: Wide DataFrame with date + symbol columns
        scale: Target sum of absolute values (default: 1.0). When longscale or
            shortscale are specified, this is ignored.
        longscale: Target sum of positive values (default: 0.0, meaning no scaling).
            When > 0, positive values are scaled so their sum equals this value.
        shortscale: Target sum of absolute negative values (default: 0.0, meaning
            no scaling). When > 0, negative values are scaled so sum(abs(neg)) equals
            this value.

    Returns:
        Wide DataFrame with scaled values

    Examples:
        >>> scale(returns, scale=4)  # Scale to book size 4
        >>> scale(returns, scale=1) + scale(close, scale=20)  # Combine scaled alphas
        >>> scale(returns, longscale=4, shortscale=3)  # Asymmetric long/short scaling
    """
    value_cols = _get_value_cols(x)

    # Check if using long/short scaling
    use_asymmetric = longscale > 0 or shortscale > 0

    if use_asymmetric:
        # Scale long and short positions separately
        # Sum of positive values across row
        long_sum = pl.sum_horizontal(
            *[pl.when(pl.col(c) > 0).then(pl.col(c)).otherwise(0.0) for c in value_cols]
        )
        # Sum of absolute negative values across row
        short_sum = pl.sum_horizontal(
            *[pl.when(pl.col(c) < 0).then(-pl.col(c)).otherwise(0.0) for c in value_cols]
        )

        # Scale factors (avoid division by zero)
        long_factor = pl.when(long_sum > 0).then(longscale / long_sum).otherwise(0.0)
        short_factor = pl.when(short_sum > 0).then(shortscale / short_sum).otherwise(0.0)

        return x.with_columns([
            pl.when(pl.col(c) > 0)
            .then(pl.col(c) * long_factor)
            .when(pl.col(c) < 0)
            .then(pl.col(c) * short_factor)
            .otherwise(0.0)
            .alias(c)
            for c in value_cols
        ])
    else:
        # Standard scaling: sum of absolute values equals scale
        abs_sum = pl.sum_horizontal(*[pl.col(c).abs() for c in value_cols])

        return x.with_columns([
            (pl.col(c) * scale / abs_sum).alias(c)
            for c in value_cols
        ])
