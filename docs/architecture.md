# Architecture

QuantDL is a financial data library for alpha research. It fetches data from S3, returns wide tables, and provides composable operators.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         QuantDLClient                                │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────────────┐ │
│  │  ticks() │  │fundament-│  │ metrics()│  │ resolve() / universe()│ │
│  │          │  │  als()   │  │          │  │                      │ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └──────────┬───────────┘ │
│       │             │             │                   │             │
│       └─────────────┴─────────────┴───────────────────┘             │
│                              │                                       │
│  ┌───────────────────────────┴───────────────────────────┐          │
│  │                   SecurityMaster                       │          │
│  │           Symbol → security_id resolution              │          │
│  └───────────────────────────┬───────────────────────────┘          │
│                              │                                       │
│  ┌───────────────────────────┴───────────────────────────┐          │
│  │              S3StorageBackend + DiskCache              │          │
│  │         Fetch parquet, cache locally, pivot wide       │          │
│  └───────────────────────────┬───────────────────────────┘          │
│                              │                                       │
│  ┌───────────────────────────┴───────────────────────────┐          │
│  │                    CalendarMaster                      │          │
│  │              Align rows to trading days                │          │
│  └───────────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   Wide DataFrame    │
                    │ timestamp | symbols │
                    └─────────────────────┘
                               │
                               ▼
         ┌─────────────────────┴─────────────────────┐
         │                                           │
         ▼                                           ▼
┌─────────────────┐                       ┌─────────────────┐
│    Operators    │                       │    Alpha DSL    │
│  ts_mean, rank  │                       │  Alpha class    │
│  zscore, etc.   │                       │  alpha_eval()   │
└─────────────────┘                       └─────────────────┘
```

---

## Module Structure

```
src/quantdl/
├── client.py               # QuantDLClient: main entry point
├── storage/
│   ├── s3.py               # S3StorageBackend: parquet I/O, request counting
│   └── cache.py            # DiskCache: LRU disk cache with TTL
├── data/
│   ├── security_master.py  # Symbol → security_id resolution
│   └── calendar_master.py  # Trading day lookups
├── operators/
│   ├── time_series.py      # ts_mean, ts_delta, ts_corr, etc.
│   ├── cross_sectional.py  # rank, zscore, scale, winsorize
│   ├── arithmetic.py       # add, multiply, power, log
│   ├── logical.py          # lt, gt, eq, if_else, is_nan
│   ├── group.py            # group_rank, group_zscore
│   └── vector.py           # vec_avg, vec_sum
├── alpha/
│   ├── core.py             # Alpha class with operator overloading
│   └── parser.py           # alpha_eval() string DSL
├── types.py                # SecurityInfo dataclass
└── exceptions.py           # Custom exceptions
```

---

## Core Components

### QuantDLClient

**Location**: `src/quantdl/client.py`

Main entry point for data access. Orchestrates fetching, caching, and alignment.

| Method | Description |
|--------|-------------|
| `ticks()` | Fetch OHLCV price data |
| `fundamentals()` | Fetch SEC filing data (quarterly or TTM) |
| `metrics()` | Fetch derived ratios (PE, PB, ROE, etc.) |
| `universe()` | Load symbol universe |
| `resolve()` | Resolve symbol to SecurityInfo |
| `request_count()` | Get S3 request count |
| `request_stats()` | Get detailed request statistics |

**Concurrency**: Uses `ThreadPoolExecutor` + `asyncio` for parallel S3 fetches.

### S3StorageBackend

**Location**: `src/quantdl/storage/s3.py`

Handles S3 I/O with Polars native `scan_parquet`.

| Feature | Description |
|---------|-------------|
| Local mode | `local_path` param bypasses S3 for testing |
| Request counting | Tracks session and daily request counts |
| Credential handling | Supports explicit creds or environment variables |

### DiskCache

**Location**: `src/quantdl/storage/cache.py`

LRU disk cache with configurable TTL and size limits.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `cache_dir` | `~/.quantdl/cache` | Cache directory |
| `ttl_seconds` | 86400 (24h) | Time-to-live |
| `max_size_bytes` | 10GB | Max cache size |

### SecurityMaster

**Location**: `src/quantdl/data/security_master.py`

Resolves symbols to security IDs with point-in-time accuracy.

| Input | Output |
|-------|--------|
| Symbol (AAPL) | SecurityInfo with security_id, CIK, etc. |
| CIK | SecurityInfo |
| security_id | SecurityInfo |

**Point-in-time**: Handles ticker changes and corporate actions correctly.

### CalendarMaster

**Location**: `src/quantdl/data/calendar_master.py`

Provides trading day lookups for date alignment.

| Method | Description |
|--------|-------------|
| `get_trading_days(start, end)` | List of trading days in range |
| `is_trading_day(date)` | Check if date is a trading day |

---

## Data Flow

### 1. Request Phase

```
client.ticks(["AAPL", "MSFT"], "close", "2024-01-01", "2024-12-31")
       │
       ▼
┌─────────────────────────────────────────┐
│  SecurityMaster.resolve()               │
│  "AAPL" → SecurityInfo(security_id=...) │
│  "MSFT" → SecurityInfo(security_id=...) │
└─────────────────────────────────────────┘
```

### 2. Fetch Phase

```
┌─────────────────────────────────────────────────────────────┐
│  ThreadPoolExecutor (max_concurrency=10)                    │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ Fetch AAPL  │  │ Fetch MSFT  │  │ Fetch ...   │         │
│  │ (parallel)  │  │ (parallel)  │  │ (parallel)  │         │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘         │
│         │                │                │                 │
│         └────────────────┴────────────────┘                 │
│                          │                                  │
│                          ▼                                  │
│  ┌─────────────────────────────────────────┐               │
│  │  DiskCache.get() or S3StorageBackend    │               │
│  │  read_parquet()                          │               │
│  └─────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────┘
```

### 3. Transform Phase

```
┌─────────────────────────────────────────┐
│  Long Format (per-security)             │
│  ┌─────────┬────────┬───────┐          │
│  │timestamp│security│ close │          │
│  │2024-01-02│ AAPL  │ 185.5 │          │
│  │2024-01-02│ MSFT  │ 375.0 │          │
│  └─────────┴────────┴───────┘          │
└────────────────┬────────────────────────┘
                 │ pivot()
                 ▼
┌─────────────────────────────────────────┐
│  Wide Format                            │
│  ┌─────────┬───────┬───────┐           │
│  │timestamp│ AAPL  │ MSFT  │           │
│  │2024-01-02│ 185.5 │ 375.0 │           │
│  │2024-01-03│ 186.0 │ 376.5 │           │
│  └─────────┴───────┴───────┘           │
└────────────────┬────────────────────────┘
                 │ align_to_calendar()
                 ▼
┌─────────────────────────────────────────┐
│  Aligned to Trading Days                │
│  (fills missing rows from calendar)     │
└─────────────────────────────────────────┘
```

---

## S3 Bucket Structure

Bucket: `us-equity-datalake`

```
data/
├── raw/
│   ├── ticks/daily/{security_id}/history.parquet   # OHLCV
│   └── fundamental/{cik}/fundamental.parquet       # SEC filings (quarterly)
├── derived/
│   └── features/fundamental/{cik}/
│       ├── ttm.parquet                             # Trailing twelve months
│       └── metrics.parquet                         # Derived ratios
├── master/
│   ├── security_master.parquet                     # Symbol ↔ security_id
│   └── calendar_master.parquet                     # Trading days
└── universe/
    └── {name}.parquet                              # Symbol universes
```

---

## Wide Table Format

All data methods return wide Polars DataFrames:

```
┌────────────┬────────┬────────┬────────┐
│ timestamp  │ AAPL   │ MSFT   │ GOOGL  │
│ date       │ f64    │ f64    │ f64    │
╞════════════╪════════╪════════╪════════╡
│ 2024-01-02 │ 185.50 │ 375.00 │ 140.25 │
│ 2024-01-03 │ 186.00 │ 376.50 │ 141.00 │
│ 2024-01-04 │ 184.25 │ 374.00 │ 139.50 │
└────────────┴────────┴────────┴────────┘
```

**Why wide format?**
- Time-series operators work column-wise (each symbol independently)
- Cross-sectional operators work row-wise (across symbols per day)
- Efficient for alpha calculations with Polars expressions

---

## Operator Architecture

### Categories

| Category | Direction | Example |
|----------|-----------|---------|
| Time-series | Column-wise | `ts_mean(df, 20)` - 20-day rolling mean per symbol |
| Cross-sectional | Row-wise | `rank(df)` - rank across symbols per day |
| Arithmetic | Element-wise | `add(df1, df2)` - element-wise addition |
| Logical | Element-wise | `if_else(cond, x, y)` - conditional |
| Group | Grouped rows | `group_rank(df, group)` - rank within groups |

### Composition

Operators are designed to be composed:

```python
# Rank of z-scored 20-day momentum
alpha = rank(zscore(ts_delta(prices, 20)))
```

All operators:
- Take wide DataFrames as input
- Return wide DataFrames as output
- Preserve the timestamp column
- Handle NaN values appropriately

---

## Alpha DSL Architecture

### Alpha Class

**Location**: `src/quantdl/alpha/core.py`

Wrapper around DataFrame enabling Python operator overloading:

```python
close = Alpha(close_df)
volume = Alpha(volume_df)
signal = close * volume - 100  # Uses __mul__, __sub__
```

### alpha_eval

**Location**: `src/quantdl/alpha/parser.py`

Safe AST-based expression evaluator:

```
"rank(-ts_delta(close, 5))"
         │
         ▼ ast.parse()
┌─────────────────────────────┐
│ AST: Call(rank, UnaryOp(-,  │
│      Call(ts_delta, ...)))  │
└─────────────────────────────┘
         │
         ▼ SafeEvaluator.visit()
┌─────────────────────────────┐
│ Resolve variables           │
│ Inject ops functions        │
│ Execute operators           │
└─────────────────────────────┘
         │
         ▼
    Alpha result
```

**Security**: Only allows whitelisted operations (no exec/eval).

---

## Caching Strategy

### Two-Level Cache

1. **Disk Cache**: LRU cache on local filesystem
2. **In-Memory**: SecurityMaster and CalendarMaster cache lookups

### Cache Keys

| Data Type | Cache Key |
|-----------|-----------|
| Ticks | `data/raw/ticks/daily/{security_id}/history.parquet` |
| Fundamentals | `data/raw/fundamental/{cik}/fundamental.parquet` |
| TTM | `data/derived/features/fundamental/{cik}/ttm.parquet` |
| Metrics | `data/derived/features/fundamental/{cik}/metrics.parquet` |

### Cache Behavior

- **Write-through**: Data cached after S3 fetch
- **TTL-based expiry**: Default 24 hours
- **LRU eviction**: Oldest entries removed when max size reached

---

## Testing Architecture

### Local Mode

Tests use `local_data_path` to bypass S3:

```python
client = QuantDLClient(local_data_path="tests/fixtures/data")
```

### Fixtures

**Location**: `tests/conftest.py`

Provides sample data fixtures:
- `sample_ticks` - OHLCV data
- `sample_fundamentals` - SEC filing data
- `sample_wide_df` - Pre-pivoted wide table

### Mocking

Uses `moto` for S3 mocking in integration tests.

---

## Error Handling

### Exception Hierarchy

```
Exception
└── QuantDLError (base)
    ├── DataNotFoundError
    │   └── Raised when data doesn't exist in S3
    ├── AlphaError (base)
    │   ├── AlphaParseError
    │   │   └── Invalid DSL expression
    │   ├── ColumnMismatchError
    │   │   └── DataFrames have different columns
    │   └── DateMismatchError
    │       └── DataFrames have different rows
    └── ValidationError
        └── Invalid input parameters
```

---

## Performance Considerations

### Concurrency

- Default `max_concurrency=10` for S3 requests
- ThreadPoolExecutor for I/O-bound operations
- asyncio for coordination

### Polars Optimization

- Native `scan_parquet` for lazy evaluation
- Column pruning (only fetch needed fields)
- Predicate pushdown (date filtering)

### Memory Management

- Chunked fetching for large universes
- Streaming parquet reads where possible
- Cache eviction prevents unbounded growth
