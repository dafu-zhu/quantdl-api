# Alpha DSL

Domain-specific language for composing alpha expressions with operator overloading.

## Overview

The Alpha DSL provides two ways to build alpha expressions:

1. **Alpha class** - Python operator overloading (`+`, `-`, `*`, `/`, `<`, `>`, etc.)
2. **alpha_eval()** - String-based DSL for dynamic expressions

Both work on **wide DataFrames**: first column is date, remaining columns are symbols.

```
timestamp   | AAPL  | MSFT  | GOOGL
2024-01-01  | 100.0 | 200.0 | 150.0
2024-01-02  | 101.0 | 198.0 | 152.0
```

## Installation

```python
from quantdl.alpha import Alpha, alpha_eval
import quantdl.operators as ops
```

## Alpha Class

Wrap a DataFrame to enable operator overloading:

```python
close = Alpha(close_df)
volume = Alpha(volume_df)
```

### Arithmetic

```python
returns = close / ops.ts_delay(close.data, 1) - 1    # daily returns
weighted = returns * volume                           # volume-weighted
scaled = returns * 100                                # scalar multiply
```

### Comparisons

Returns `1.0` for True, `0.0` for False:

```python
mask = close > 100          # Alpha with 1.0 where close > 100
mask = close >= volume      # element-wise comparison
```

### Logical Operations

```python
both = (close > 100) & (volume > 1000)    # AND
either = (close > 100) | (volume > 1000)  # OR
inverted = ~(close > 100)                  # NOT (1.0 where False)
```

### Unary Operations

```python
neg = -close          # negation
absolute = abs(close) # absolute value (Python builtin works)
```

### Access Underlying Data

```python
alpha = close * 2
result_df = alpha.data  # get pl.DataFrame
```

## Operators Integration

Operators work on raw DataFrames. Use `.data` to extract from Alpha:

```python
close = Alpha(close_df)

# Apply operator to underlying DataFrame
ma = ops.ts_mean(close.data, 20)

# Wrap result back in Alpha for further operations
ma_alpha = Alpha(ma)
signal = ma_alpha - close
```

### Common Pattern

```python
# Compute momentum, rank cross-sectionally
momentum = ops.ts_delta(close_df, 5)
ranked = ops.rank(momentum)
signal = Alpha(ranked)
```

### Chaining Operators

```python
# Rank of z-scored 20-day returns
returns = close_df.select(
    pl.col("timestamp"),
    *[(pl.col(c) / pl.col(c).shift(20) - 1) for c in symbols]
)
alpha = ops.rank(ops.zscore(returns))
```

## String DSL (alpha_eval)

Parse and evaluate string expressions safely using AST:

```python
result = alpha_eval(
    "close * 2 + 1",
    {"close": close_df}
)
```

### With Operators

Access operators via `ops.` prefix:

```python
result = alpha_eval(
    "ops.rank(-ops.ts_delta(close, 5))",
    {"close": close_df},
    ops=ops
)
```

### Builtin Functions

Available without `ops.` prefix: `abs`, `min`, `max`, `log`, `sqrt`, `sign`

```python
result = alpha_eval("min(close, vwap)", {"close": close_df, "vwap": vwap_df})
result = alpha_eval("abs(close - 100)", {"close": close_df})
```

### Ternary Expressions

```python
result = alpha_eval(
    "close if close > 100 else 0",
    {"close": close_df}
)
```

### Complex Expressions

```python
result = alpha_eval(
    "ops.rank(ops.ts_mean(close, 5)) * (volume > ops.ts_mean(volume, 20))",
    {"close": close_df, "volume": volume_df},
    ops=ops
)
```

## Validation

Alpha operations validate alignment automatically:

```python
from quantdl.alpha import ColumnMismatchError, DateMismatchError

try:
    result = Alpha(df1) + Alpha(df2)  # different columns
except ColumnMismatchError as e:
    print(e.left_cols, e.right_cols)

try:
    result = Alpha(df1) + Alpha(df3)  # different row counts
except DateMismatchError as e:
    print(e.left_dates, e.right_dates)
```

Scalar operations don't require alignment:

```python
result = Alpha(close_df) * 2  # always works
```

## Type Reference

| Type | Description |
|------|-------------|
| `Alpha` | Wrapped DataFrame with operator overloading |
| `AlphaLike` | `Alpha`, `pl.DataFrame`, `int`, or `float` |
| `Scalar` | `int` or `float` |

## Operator Categories

| Category | Examples | Input |
|----------|----------|-------|
| Time-series | `ts_mean`, `ts_delta`, `ts_rank` | column-wise |
| Cross-sectional | `rank`, `zscore`, `scale` | row-wise |
| Arithmetic | `abs`, `add`, `multiply`, `log` | element-wise |
| Logical | `and_`, `or_`, `if_else` | element-wise |
| Group | `group_rank`, `group_zscore` | grouped rows |

## Example: Momentum Alpha

```python
from quantdl.alpha import Alpha
import quantdl.operators as ops

# Load data
close = client.daily("close", symbols, start, end)
volume = client.daily("volume", symbols, start, end)

# 5-day momentum, ranked
momentum = ops.ts_delta(close, 5)
ranked = ops.rank(momentum)

# Filter by volume
avg_volume = ops.ts_mean(volume, 20)
mask = (volume > avg_volume).cast(pl.Float64)

# Final alpha
alpha = Alpha(ranked) * Alpha(mask)
result = alpha.data
```

## Example: Mean Reversion Alpha

```python
# Z-score of price vs 20-day MA
ma = ops.ts_mean(close, 20)
std = ops.ts_std(close, 20)
zscore = ops.divide(ops.subtract(close, ma), std)

# Negative zscore = buy signal (mean reversion)
alpha = ops.rank(-zscore)
```
