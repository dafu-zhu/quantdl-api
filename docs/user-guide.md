# User Guide

Complete guide to using QuantDL for financial data access and alpha research.

## Installation

```bash
pip install quantdl
```

Or with uv:
```bash
uv add quantdl
```

## Quick Start

```python
from quantdl import QuantDLClient
from quantdl.operators import ts_mean, ts_delta, rank, zscore

# Initialize client
client = QuantDLClient()

# Get daily closing prices
prices = client.ticks(["AAPL", "MSFT", "GOOGL"], "close", "2024-01-01", "2024-12-31")

# Apply operators
momentum = ts_delta(prices, 5)      # 5-day price change
ranked = rank(momentum)              # Cross-sectional rank
alpha = zscore(ranked)               # Standardize

print(alpha)
```

---

## Configuration

### AWS Credentials

Set credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

Or pass explicitly:

```python
client = QuantDLClient(
    aws_access_key_id="your_key",
    aws_secret_access_key="your_secret",
    aws_region="us-east-1",
)
```

### Client Options

```python
client = QuantDLClient(
    bucket="us-equity-datalake",       # S3 bucket
    cache_dir="~/.quantdl/cache",      # Local cache directory
    cache_ttl_seconds=86400,           # Cache TTL (24 hours)
    cache_max_size_bytes=10*1024**3,   # Max cache size (10GB)
    max_concurrency=10,                # Concurrent S3 requests
)
```

### Local Testing Mode

For testing without S3:

```python
client = QuantDLClient(local_data_path="/path/to/local/data")
```

---

## Fetching Data

### Price Data (Ticks)

```python
# Single symbol
aapl = client.ticks("AAPL", "close", "2024-01-01", "2024-12-31")

# Multiple symbols
prices = client.ticks(
    symbols=["AAPL", "MSFT", "GOOGL"],
    field="close",                     # open, high, low, close, volume
    start="2024-01-01",
    end="2024-12-31"
)
```

**Output**: Wide DataFrame with timestamp as first column, symbols as other columns.

```
┌────────────┬────────┬────────┬────────┐
│ timestamp  │ AAPL   │ MSFT   │ GOOGL  │
│ date       │ f64    │ f64    │ f64    │
╞════════════╪════════╪════════╪════════╡
│ 2024-01-02 │ 185.50 │ 375.00 │ 140.25 │
│ 2024-01-03 │ 186.00 │ 376.50 │ 141.00 │
└────────────┴────────┴────────┴────────┘
```

### Fundamental Data

```python
# Revenue (income statement - uses TTM by default)
revenue = client.fundamentals(
    symbols=["AAPL", "MSFT"],
    concept="rev",                     # rev, net_inc, op_inc, etc.
    start="2024-01-01",
    end="2024-12-31"
)

# Total assets (balance sheet - uses quarterly by default)
assets = client.fundamentals(
    symbols=["AAPL", "MSFT"],
    concept="ta",
    start="2024-01-01",
    end="2024-12-31"
)

# Force quarterly or TTM
quarterly_rev = client.fundamentals(
    symbols=["AAPL"],
    concept="rev",
    source="raw"                       # "raw" = quarterly, "ttm" = trailing 12mo
)
```

**Duration concepts** (default to TTM): `rev`, `cor`, `op_inc`, `net_inc`, `ibt`, `inc_tax_exp`, `int_exp`, `rnd`, `sga`, `dna`, `cfo`, `cfi`, `cff`, `capex`, `div`, `sto_isu`

**Balance sheet concepts** (default to quarterly): `ta`, `tl`, `te`, `ca`, `cl`, etc.

### Derived Metrics

```python
pe = client.metrics(
    symbols=["AAPL", "MSFT"],
    metric="pe_ratio",                 # pe_ratio, pb_ratio, roe, roa
    start="2024-01-01",
    end="2024-12-31"
)
```

### Symbol Resolution

```python
from datetime import date

# Resolve symbol to security info
info = client.resolve("AAPL", as_of=date(2024, 1, 1))
print(info.security_id)  # Internal ID
print(info.cik)          # SEC CIK number

# Load universe
symbols = client.universe("top3000")
print(len(symbols))  # 3000 symbols
```

---

## Operators

### Time-Series Operators (Column-wise)

Work independently on each symbol column:

```python
from quantdl.operators import (
    ts_mean, ts_sum, ts_std, ts_min, ts_max,
    ts_delta, ts_delay, ts_rank, ts_zscore,
    ts_corr, ts_covariance, ts_regression
)

# Rolling statistics
ma_20 = ts_mean(prices, 20)           # 20-day moving average
vol_20 = ts_std(prices, 20)           # 20-day volatility
rolling_max = ts_max(prices, 52)      # 52-day high

# Differences and lags
momentum = ts_delta(prices, 5)        # 5-day change (price - price_5d_ago)
lagged = ts_delay(prices, 1)          # Yesterday's price

# Time-series rank
ts_pct_rank = ts_rank(prices, 20)     # Percentile rank in last 20 days

# Z-score over time
ts_z = ts_zscore(prices, 20)          # (x - mean) / std over 20 days

# Two-variable operators
corr = ts_corr(prices, volume, 20)    # Rolling correlation
cov = ts_covariance(prices, volume, 20)
```

### Cross-Sectional Operators (Row-wise)

Work across symbols for each day:

```python
from quantdl.operators import rank, zscore, normalize, scale, winsorize, quantile

# Rank across symbols (0 to 1)
ranked = rank(prices)                  # Highest price gets 1.0

# Z-score across symbols
standardized = zscore(prices)          # (x - mean) / std per row

# Normalize (demean)
demeaned = normalize(prices)           # x - row_mean

# Scale to target book size
scaled = scale(prices, scale=1.0)      # |sum| = 1 (dollar-neutral)

# Winsorize outliers
clipped = winsorize(prices, std=3)     # Clip beyond 3 std

# Quantile transform
quantiled = quantile(prices)           # Map to gaussian quantiles
```

### Arithmetic Operators

```python
from quantdl.operators import add, subtract, multiply, divide, power, log, sqrt, abs, sign

# Element-wise operations
returns = divide(ts_delta(prices, 1), ts_delay(prices, 1))
log_prices = log(prices)
volatility = sqrt(ts_mean(power(returns, 2), 20))

# With filter for NaN handling
total = add(a, b, filter=True)         # NaN treated as 0
product = multiply(a, b, filter=True)  # NaN treated as 1
```

### Logical Operators

```python
from quantdl.operators import lt, gt, eq, if_else, is_nan, and_, or_, not_

# Comparisons (return 1.0 for True, 0.0 for False)
above_ma = gt(prices, ts_mean(prices, 20))
below_ma = lt(prices, ts_mean(prices, 20))

# Conditional
filtered = if_else(above_ma, prices, 0)  # Zero out below MA

# Combine conditions
both = and_(above_ma, gt(volume, ts_mean(volume, 20)))
```

### Group Operators

```python
from quantdl.operators import group_rank, group_zscore, group_neutralize

# Rank within sector
sector_rank = group_rank(momentum, sector_df)

# Z-score within sector
sector_z = group_zscore(momentum, sector_df)

# Neutralize against sector
neutral = group_neutralize(alpha, sector_df)
```

---

## Alpha DSL

### Using the Alpha Class

```python
from quantdl.alpha import Alpha
import quantdl.operators as ops

# Wrap DataFrames for operator overloading
close = Alpha(close_df)
volume = Alpha(volume_df)

# Arithmetic
returns = close / Alpha(ops.ts_delay(close.data, 1)) - 1

# Comparisons (return 1.0/0.0)
vol_filter = volume > Alpha(ops.ts_mean(volume.data, 20))

# Combine
signal = Alpha(ops.rank(returns.data)) * vol_filter

# Get result DataFrame
result = signal.data
```

### Using String DSL (alpha_eval)

For dynamic expressions (GP/RL compatible):

```python
from quantdl.alpha import alpha_eval
import quantdl.operators as ops

# Clean syntax (recommended)
alpha = alpha_eval(
    "rank(-ts_delta(close, 5))",
    {"close": close_df},
    ops=ops
)

# Complex expression
alpha = alpha_eval(
    "rank(ts_zscore(close, 20)) * (volume > ts_mean(volume, 20))",
    {"close": close_df, "volume": volume_df},
    ops=ops
)

# Ternary
alpha = alpha_eval(
    "close if volume > 0 else 0",
    {"close": close_df, "volume": volume_df}
)
```

---

## Common Patterns

### Momentum Alpha

```python
from quantdl import QuantDLClient
from quantdl.operators import ts_delta, ts_delay, rank

client = QuantDLClient()
symbols = client.universe("top3000")

# Get data
close = client.ticks(symbols, "close", "2024-01-01", "2024-12-31")

# 5-day momentum, ranked
momentum = ts_delta(close, 5)
alpha = rank(momentum)  # Higher momentum = higher rank
```

### Mean Reversion Alpha

```python
from quantdl.operators import ts_mean, ts_std, divide, subtract, rank

# Z-score relative to 20-day mean
ma = ts_mean(close, 20)
std = ts_std(close, 20)
zscore = divide(subtract(close, ma), std)

# Negative z-score = buy signal (expect reversion up)
alpha = rank(-zscore)
```

### Volume-Filtered Alpha

```python
from quantdl.alpha import Alpha
from quantdl.operators import ts_mean, ts_delta, rank, gt

close = client.ticks(symbols, "close", "2024-01-01", "2024-12-31")
volume = client.ticks(symbols, "volume", "2024-01-01", "2024-12-31")

# Momentum signal
momentum = ts_delta(close, 5)
signal = rank(momentum)

# Volume filter: only trade high-volume stocks
vol_filter = gt(volume, ts_mean(volume, 20))

# Apply filter
alpha = Alpha(signal) * Alpha(vol_filter)
result = alpha.data
```

### Sector-Neutral Alpha

```python
from quantdl.operators import ts_delta, rank, group_neutralize

# Raw momentum
momentum = rank(ts_delta(close, 20))

# Neutralize against sector
sector = client.ticks(symbols, "sector", "2024-01-01", "2024-12-31")
alpha = group_neutralize(momentum, sector)
```

### Combining Multiple Signals

```python
from quantdl.alpha import Alpha
from quantdl.operators import rank, ts_delta, ts_mean, ts_std

# Signal 1: Momentum
momentum = rank(ts_delta(close, 5))

# Signal 2: Mean reversion
ma = ts_mean(close, 20)
std = ts_std(close, 20)
reversion = rank(-(close - ma) / std)

# Combine with equal weights
alpha = Alpha(momentum) * 0.5 + Alpha(reversion) * 0.5
```

---

## Caching and Performance

### Check Cache Stats

```python
stats = client.cache_stats()
print(f"Cache hits: {stats['hits']}")
print(f"Cache misses: {stats['misses']}")
print(f"Cache size: {stats['size_bytes'] / 1024**2:.1f} MB")
```

### Clear Cache

```python
client.clear_cache()
```

### Monitor S3 Requests

```python
# Session request count
print(f"Requests this session: {client.request_count()}")

# Today's request count
print(f"Requests today: {client.request_count('today')}")

# Detailed stats
stats = client.request_stats()
print(stats)
```

### Performance Tips

1. **Reuse client**: Create one client and reuse it
2. **Batch fetches**: Fetch multiple symbols in one call
3. **Use cache**: Default 24h cache avoids redundant S3 requests
4. **Date ranges**: Only fetch the data you need

```python
# Good: Single fetch for all symbols
prices = client.ticks(symbols, "close", start, end)

# Bad: Multiple fetches
for sym in symbols:
    price = client.ticks(sym, "close", start, end)  # N requests vs 1
```

---

## Error Handling

```python
from quantdl.exceptions import DataNotFoundError
from quantdl.alpha import AlphaParseError, ColumnMismatchError

# Handle missing data
try:
    prices = client.ticks(["INVALID"], "close", "2024-01-01", "2024-12-31")
except DataNotFoundError as e:
    print(f"Data not found: {e}")

# Handle DSL errors
try:
    alpha = alpha_eval("invalid_func(x)", {"x": df})
except AlphaParseError as e:
    print(f"Parse error: {e}")

# Handle misaligned DataFrames
try:
    result = Alpha(df1) + Alpha(df2)  # Different columns
except ColumnMismatchError as e:
    print(f"Column mismatch: {e.left_cols} vs {e.right_cols}")
```

---

## Context Manager

Use client as context manager to ensure cleanup:

```python
with QuantDLClient() as client:
    prices = client.ticks(["AAPL"], "close", "2024-01-01", "2024-12-31")
    # Executor cleaned up on exit
```

---

## Full Example: Research Workflow

```python
from quantdl import QuantDLClient
from quantdl.alpha import Alpha, alpha_eval
from quantdl.operators import (
    ts_mean, ts_std, ts_delta, ts_delay,
    rank, zscore, scale, winsorize
)
import polars as pl

# Initialize
client = QuantDLClient()

# Load universe
symbols = client.universe("top3000")[:100]  # Use 100 for demo

# Date range
start, end = "2024-01-01", "2024-06-30"

# Fetch data
close = client.ticks(symbols, "close", start, end)
volume = client.ticks(symbols, "volume", start, end)

# Calculate returns
returns = close.select(
    pl.col("timestamp"),
    *[(pl.col(c) / pl.col(c).shift(1) - 1).alias(c)
      for c in close.columns[1:]]
)

# Build alpha: momentum * volume filter * volatility adjustment
momentum = ts_delta(close, 20)
momentum_rank = rank(momentum)

vol_ma = ts_mean(volume, 20)
vol_filter = Alpha(volume) > Alpha(vol_ma)

volatility = ts_std(returns, 20)
vol_adjust = 1 / Alpha(volatility)

raw_alpha = Alpha(momentum_rank) * vol_filter * vol_adjust

# Clean up
alpha = winsorize(raw_alpha.data, std=3)  # Clip outliers
alpha = scale(alpha, scale=1.0)            # Dollar-neutral

print(f"Alpha shape: {alpha.shape}")
print(alpha.head())

# Check request stats
print(f"S3 requests: {client.request_count()}")
```
