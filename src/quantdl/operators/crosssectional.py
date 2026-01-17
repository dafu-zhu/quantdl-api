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


def scale(x: pl.DataFrame, target: float = 1.0) -> pl.DataFrame:
    """Scale values so that sum of absolute values equals target.

    Useful for creating dollar-neutral portfolios.

    Args:
        x: Wide DataFrame with date + symbol columns
        target: Target sum of absolute values (default: 1.0)

    Returns:
        Wide DataFrame with scaled values
    """
    value_cols = _get_value_cols(x)

    # Sum of absolute values across row
    abs_sum = pl.sum_horizontal(*[pl.col(c).abs() for c in value_cols])

    return x.with_columns([
        (pl.col(c) * target / abs_sum).alias(c)
        for c in value_cols
    ])
