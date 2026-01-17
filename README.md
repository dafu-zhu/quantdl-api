# QuantDL

Financial data library for alpha research. Fetches data from S3, returns wide tables, includes local caching and composable operators.

## Installation

```bash
pip install quantdl
```

## Quick Start

```python
from quantdl import QuantDLClient
from quantdl.operators import ts_mean, rank, zscore

# Initialize client
client = QuantDLClient()

# Get daily closing prices for multiple symbols
prices = client.daily(["AAPL", "MSFT", "GOOGL"], "close", "2024-01-01", "2024-12-31")
# Returns wide table: timestamp | AAPL | MSFT | GOOGL

# Apply operators
ma_20 = ts_mean(prices, 20)      # 20-day moving average
ranked = rank(ma_20)              # Cross-sectional rank
standardized = zscore(ma_20)      # Cross-sectional z-score
```

## Features

- **Wide Table Format**: Returns DataFrames with dates as rows, symbols as columns
- **Local Caching**: LRU disk cache with configurable TTL (default 24h, 10GB)
- **Point-in-Time Resolution**: Handles symbol changes and corporate actions
- **Alpha Operators**: Time-series and cross-sectional transformations
- **Polars-Native**: Built on Polars for fast data operations

## API Reference

### Client

```python
client = QuantDLClient(
    bucket="us-equity-datalake",       # S3 bucket
    cache_dir="~/.quantdl/cache",      # Local cache directory
    cache_ttl_seconds=86400,           # Cache TTL (24 hours)
    cache_max_size_bytes=10*1024**3,   # Max cache size (10GB)
    max_concurrency=10,                # Concurrent S3 requests
)
```

### Data Methods

```python
# Daily prices (OHLCV)
df = client.daily(
    symbols=["AAPL", "MSFT"],   # Symbol(s)
    field="close",              # open, high, low, close, volume
    start="2024-01-01",
    end="2024-12-31"
)

# Fundamentals
df = client.fundamentals(
    symbols=["AAPL"],
    concept="Revenue",          # Revenue, NetIncome, etc.
    start="2024-01-01",
    end="2024-12-31"
)

# Derived metrics
df = client.metrics(
    symbols=["AAPL"],
    metric="pe_ratio",          # pe_ratio, pb_ratio, roe, roa
    start="2024-01-01",
    end="2024-12-31"
)

# Load universe
symbols = client.universe("top3000")

# Resolve symbol to security info
info = client.resolve("AAPL", as_of=date(2024, 1, 1))
```

### Operators

#### Time-Series (Column-wise)

```python
from quantdl.operators import ts_mean, ts_sum, ts_std, ts_min, ts_max, ts_delta, ts_delay

ts_mean(df, 20)    # 20-day rolling mean
ts_sum(df, 10)     # 10-day rolling sum
ts_std(df, 20)     # 20-day rolling standard deviation
ts_min(df, 20)     # 20-day rolling minimum
ts_max(df, 20)     # 20-day rolling maximum
ts_delta(df, 1)    # 1-day difference
ts_delay(df, 5)    # Lag by 5 days
```

#### Cross-Sectional (Row-wise)

```python
from quantdl.operators import rank, zscore, demean, scale

rank(df)            # Cross-sectional rank (1 to N)
zscore(df)          # Cross-sectional z-score
demean(df)          # Subtract row mean
scale(df, 1.0)      # Scale so |sum| = 1 (dollar-neutral)
```

#### Composing Operators

```python
# Alpha = rank(20-day momentum z-score)
momentum = ts_delta(prices, 20) / ts_delay(prices, 20)
alpha = rank(zscore(momentum))
```

## Data Structure

All data methods return wide Polars DataFrames:

```
timestamp   | AAPL    | MSFT    | GOOGL
------------|---------|---------|--------
2024-01-02  | 185.50  | 375.00  | 140.25
2024-01-03  | 186.00  | 376.50  | 141.00
...
```

## Configuration

Set AWS credentials via environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

## Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Lint and type check
uv run ruff check .
uv run mypy src/

# Build
uv build
```

## License

MIT
