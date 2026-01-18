"""Arithmetic operators for wide tables.

All operators preserve the wide table structure:
- First column (date) is unchanged
- Operations applied element-wise to symbol columns
"""

import polars as pl


def _get_value_cols(df: pl.DataFrame) -> list[str]:
    """Get value columns (all except first which is date)."""
    return df.columns[1:]


def abs(x: pl.DataFrame) -> pl.DataFrame:
    """Absolute value.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with absolute values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    return x.select(
        pl.col(date_col),
        *[pl.col(c).abs().alias(c) for c in value_cols],
    )


def add(*args: pl.DataFrame, filter: bool = False) -> pl.DataFrame:
    """Element-wise addition of two or more DataFrames.

    Args:
        *args: Two or more wide DataFrames with matching structure
        filter: If True, treat NaN as 0

    Returns:
        Wide DataFrame with summed values
    """
    if len(args) < 2:
        raise ValueError("add requires at least 2 inputs")

    date_col = args[0].columns[0]
    value_cols = _get_value_cols(args[0])

    result = args[0]
    for df in args[1:]:
        if filter:
            result = result.select(
                pl.col(date_col),
                *[
                    (pl.col(c).fill_null(0) + df[c].fill_null(0)).alias(c)
                    for c in value_cols
                ],
            )
        else:
            result = result.select(
                pl.col(date_col),
                *[(pl.col(c) + df[c]).alias(c) for c in value_cols],
            )
    return result


def subtract(x: pl.DataFrame, y: pl.DataFrame, filter: bool = False) -> pl.DataFrame:
    """Element-wise subtraction: x - y.

    Args:
        x: Wide DataFrame with date + symbol columns
        y: Wide DataFrame with date + symbol columns
        filter: If True, treat NaN as 0

    Returns:
        Wide DataFrame with x - y values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    if filter:
        return x.select(
            pl.col(date_col),
            *[
                (pl.col(c).fill_null(0) - y[c].fill_null(0)).alias(c)
                for c in value_cols
            ],
        )
    return x.select(
        pl.col(date_col),
        *[(pl.col(c) - y[c]).alias(c) for c in value_cols],
    )


def multiply(*args: pl.DataFrame, filter: bool = False) -> pl.DataFrame:
    """Element-wise multiplication of two or more DataFrames.

    Args:
        *args: Two or more wide DataFrames with matching structure
        filter: If True, treat NaN as 1

    Returns:
        Wide DataFrame with multiplied values
    """
    if len(args) < 2:
        raise ValueError("multiply requires at least 2 inputs")

    date_col = args[0].columns[0]
    value_cols = _get_value_cols(args[0])

    result = args[0]
    for df in args[1:]:
        if filter:
            result = result.select(
                pl.col(date_col),
                *[
                    (pl.col(c).fill_null(1) * df[c].fill_null(1)).alias(c)
                    for c in value_cols
                ],
            )
        else:
            result = result.select(
                pl.col(date_col),
                *[(pl.col(c) * df[c]).alias(c) for c in value_cols],
            )
    return result


def divide(x: pl.DataFrame, y: pl.DataFrame) -> pl.DataFrame:
    """Safe element-wise division: x / y.

    Division by zero returns null.

    Args:
        x: Wide DataFrame with date + symbol columns (numerator)
        y: Wide DataFrame with date + symbol columns (denominator)

    Returns:
        Wide DataFrame with x / y values (null where y=0)
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[
            pl.when(y[c] != 0)
            .then(pl.col(c) / y[c])
            .otherwise(None)
            .alias(c)
            for c in value_cols
        ],
    )


def inverse(x: pl.DataFrame) -> pl.DataFrame:
    """Safe multiplicative inverse: 1/x.

    Division by zero returns null.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with 1/x values (null where x=0)
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[
            pl.when(pl.col(c) != 0)
            .then(1.0 / pl.col(c))
            .otherwise(None)
            .alias(c)
            for c in value_cols
        ],
    )


def log(x: pl.DataFrame) -> pl.DataFrame:
    """Natural logarithm.

    Values <= 0 return null.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with ln(x) values (null where x<=0)
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[
            pl.when(pl.col(c) > 0)
            .then(pl.col(c).log())
            .otherwise(None)
            .alias(c)
            for c in value_cols
        ],
    )


def max(*args: pl.DataFrame) -> pl.DataFrame:
    """Element-wise maximum of two or more DataFrames.

    Args:
        *args: Two or more wide DataFrames with matching structure

    Returns:
        Wide DataFrame with max values across inputs
    """
    if len(args) < 2:
        raise ValueError("max requires at least 2 inputs")

    date_col = args[0].columns[0]
    value_cols = _get_value_cols(args[0])

    return args[0].select(
        pl.col(date_col),
        *[
            pl.max_horizontal(*[df[c] for df in args]).alias(c)
            for c in value_cols
        ],
    )


def min(*args: pl.DataFrame) -> pl.DataFrame:
    """Element-wise minimum of two or more DataFrames.

    Args:
        *args: Two or more wide DataFrames with matching structure

    Returns:
        Wide DataFrame with min values across inputs
    """
    if len(args) < 2:
        raise ValueError("min requires at least 2 inputs")

    date_col = args[0].columns[0]
    value_cols = _get_value_cols(args[0])

    return args[0].select(
        pl.col(date_col),
        *[
            pl.min_horizontal(*[df[c] for df in args]).alias(c)
            for c in value_cols
        ],
    )


def power(x: pl.DataFrame, y: pl.DataFrame) -> pl.DataFrame:
    """Element-wise power: x^y.

    Args:
        x: Wide DataFrame with date + symbol columns (base)
        y: Wide DataFrame with date + symbol columns (exponent)

    Returns:
        Wide DataFrame with x^y values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[(pl.col(c).pow(y[c])).alias(c) for c in value_cols],
    )


def signed_power(x: pl.DataFrame, y: pl.DataFrame) -> pl.DataFrame:
    """Signed power: sign(x) * |x|^y.

    Preserves sign of x while raising absolute value to power y.

    Args:
        x: Wide DataFrame with date + symbol columns (base)
        y: Wide DataFrame with date + symbol columns (exponent)

    Returns:
        Wide DataFrame with sign(x) * |x|^y values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[
            (pl.col(c).sign() * pl.col(c).abs().pow(y[c])).alias(c)
            for c in value_cols
        ],
    )


def sqrt(x: pl.DataFrame) -> pl.DataFrame:
    """Square root.

    Negative values return null.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with sqrt(x) values (null where x<0)
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[
            pl.when(pl.col(c) >= 0)
            .then(pl.col(c).sqrt())
            .otherwise(None)
            .alias(c)
            for c in value_cols
        ],
    )


def sign(x: pl.DataFrame) -> pl.DataFrame:
    """Sign function: 1 for positive, -1 for negative, 0 for zero.

    Null values remain null.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with sign values (1, -1, 0, or null)
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[pl.col(c).sign().alias(c) for c in value_cols],
    )


def reverse(x: pl.DataFrame) -> pl.DataFrame:
    """Negation: -x.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with negated values
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    return x.select(
        pl.col(date_col),
        *[(-pl.col(c)).alias(c) for c in value_cols],
    )


def densify(x: pl.DataFrame) -> pl.DataFrame:
    """Remap unique values to consecutive integers 0..n-1 per row.

    Groups values by unique occurrence within each row and assigns
    sequential indices. Useful for categorical encoding.

    Args:
        x: Wide DataFrame with date + symbol columns

    Returns:
        Wide DataFrame with values remapped to 0..n-1 per row
    """
    date_col = x.columns[0]
    value_cols = _get_value_cols(x)

    # Convert to long format
    long = x.unpivot(
        index=date_col,
        on=value_cols,
        variable_name="symbol",
        value_name="value",
    )

    # Rank unique values per date using dense ranking (ties get same rank)
    ranked = long.with_columns(
        (pl.col("value").rank(method="dense").over(date_col) - 1).alias("value")
    )

    # Pivot back to wide
    wide = ranked.pivot(values="value", index=date_col, on="symbol")

    return wide.select([date_col, *value_cols])
