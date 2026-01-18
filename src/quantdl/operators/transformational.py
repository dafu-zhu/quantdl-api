"""Transformational operators for wide tables.

All operators preserve the wide table structure:
- First column (date) is unchanged
- Operations transform values into new forms (buckets, conditional signals)
"""

import polars as pl

from quantdl.exceptions import InvalidBucketSpecError


def _get_value_cols(df: pl.DataFrame) -> list[str]:
    """Get value columns (all except first which is date)."""
    return df.columns[1:]


def bucket(
    x: pl.DataFrame,
    range_spec: str | None = None,
    buckets: str | None = None,
    skipBegin: bool = False,
    skipEnd: bool = False,
    skipBoth: bool = False,
    NANGroup: bool = False,
) -> pl.DataFrame:
    """Assign values to bucket indices based on boundaries.

    Creates buckets with (lower, upper] inclusion (left-open, right-closed).
    Hidden buckets (-inf, start] and [end, +inf) added unless skipped.

    Args:
        x: Wide DataFrame with date + symbol columns
        range_spec: "start,end,step" string defining evenly spaced boundaries
        buckets: "n1,n2,..." string defining explicit boundaries
        skipBegin: If True, exclude (-inf, start] bucket
        skipEnd: If True, exclude [end, +inf) bucket
        skipBoth: If True, exclude both edge buckets (same as skipBegin + skipEnd)
        NANGroup: If True, NaN values get last_idx + 1; otherwise NaN

    Returns:
        Wide DataFrame with Int64 bucket indices

    Examples:
        >>> bucket(df, range_spec="0,10,2")  # boundaries at 0,2,4,6,8,10
        >>> bucket(df, buckets="0,5,10,20")  # explicit boundaries
    """
    if range_spec is None and buckets is None:
        raise InvalidBucketSpecError("Must specify either 'range_spec' or 'buckets'")
    if range_spec is not None and buckets is not None:
        raise InvalidBucketSpecError("Cannot specify both 'range_spec' and 'buckets'")

    # Parse boundaries
    if range_spec is not None:
        try:
            parts = range_spec.split(",")
            if len(parts) != 3:
                raise InvalidBucketSpecError(
                    f"range_spec must be 'start,end,step', got: {range_spec}"
                )
            start, end, step = float(parts[0]), float(parts[1]), float(parts[2])
            if step <= 0:
                raise InvalidBucketSpecError(f"step must be positive, got: {step}")
            boundaries: list[float] = []
            val = start
            while val <= end + 1e-9:
                boundaries.append(val)
                val += step
        except ValueError as e:
            raise InvalidBucketSpecError(f"Invalid range format: {range_spec}") from e
    else:
        try:
            assert buckets is not None  # for type checker
            boundaries = [float(b.strip()) for b in buckets.split(",")]
            if len(boundaries) < 2:
                raise InvalidBucketSpecError("buckets must have at least 2 values")
        except ValueError as e:
            raise InvalidBucketSpecError(f"Invalid buckets format: {buckets}") from e

    # Apply skip flags
    skip_begin = skipBegin or skipBoth
    skip_end = skipEnd or skipBoth

    # Build bracket list: [(lower, upper, idx), ...]
    # (lower, upper] inclusion
    brackets: list[tuple[float, float, int]] = []
    idx = 0

    # Hidden (-inf, start] bucket unless skipped
    if not skip_begin:
        brackets.append((float("-inf"), boundaries[0], idx))
        idx += 1

    # Regular buckets
    for i in range(len(boundaries) - 1):
        brackets.append((boundaries[i], boundaries[i + 1], idx))
        idx += 1

    # Hidden [end, +inf) bucket unless skipped
    if not skip_end:
        brackets.append((boundaries[-1], float("inf"), idx))
        idx += 1

    nan_idx = idx if NANGroup else None

    date_col = x.columns[0]
    value_cols = _get_value_cols(x)
    result_data: dict[str, pl.Series | list[int | None]] = {date_col: x[date_col]}

    for c in value_cols:
        col_data = x[c].to_list()
        bucket_indices: list[int | None] = []
        for val in col_data:
            if val is None or (isinstance(val, float) and val != val):  # NaN check
                bucket_indices.append(nan_idx)
            else:
                assigned = None
                for lower, upper, bucket_idx in brackets:
                    if lower < val <= upper:
                        assigned = bucket_idx
                        break
                bucket_indices.append(assigned)
        result_data[c] = bucket_indices

    return pl.DataFrame(result_data).cast(dict.fromkeys(value_cols, pl.Int64))


def trade_when(
    trigger: pl.DataFrame,
    alpha: pl.DataFrame,
    exit_trigger: pl.DataFrame,
) -> pl.DataFrame:
    """Stateful trade signal with entry/exit conditions.

    Row-by-row stateful iteration:
    - If exit_trigger > 0: alpha = NaN (exit position)
    - Else if trigger > 0: alpha = alpha_expr (enter position)
    - Else: alpha = prev_alpha (hold position)

    Exit wins over trigger when both > 0.

    Args:
        trigger: Wide DataFrame with entry trigger signals (> 0 = enter)
        alpha: Wide DataFrame with alpha values to use when triggered
        exit_trigger: Wide DataFrame with exit signals (> 0 = exit)

    Returns:
        Wide DataFrame with conditional alpha values
    """
    date_col = trigger.columns[0]
    value_cols = _get_value_cols(trigger)
    result_data: dict[str, pl.Series | list[float | None]] = {date_col: trigger[date_col]}

    for c in value_cols:
        trig_vals = trigger[c].to_list()
        alpha_vals = alpha[c].to_list()
        exit_vals = exit_trigger[c].to_list()
        n = len(trig_vals)

        out: list[float | None] = []
        prev_alpha: float | None = None

        for i in range(n):
            trig = trig_vals[i]
            alph = alpha_vals[i]
            exit_v = exit_vals[i]

            # Check exit first (exit wins over trigger)
            if exit_v is not None and exit_v > 0:
                out.append(None)
                prev_alpha = None
            # Check entry trigger
            elif trig is not None and trig > 0:
                out.append(alph)
                prev_alpha = alph
            # Hold previous position
            else:
                out.append(prev_alpha)

        result_data[c] = out

    return pl.DataFrame(result_data)
