# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build/Test Commands

```bash
uv sync --all-extras        # install deps
uv run pytest               # run all tests
uv run pytest tests/test_operators.py -k "test_rank"  # single test
uv run ruff check .         # lint
uv run mypy src/            # type check
uv build                    # build package
```

## Architecture

**QuantDL** = financial data library fetching from S3 → wide tables (timestamp rows × symbol columns) for alpha research.

```
src/quantdl/
├── client.py           # QuantDLClient: main entry point, orchestrates fetching
├── storage/
│   ├── s3.py           # S3StorageBackend: Polars native scan_parquet, request counting
│   └── cache.py        # DiskCache: LRU disk cache with TTL
├── data/
│   ├── security_master.py  # Symbol→security_id resolution (point-in-time)
│   └── calendar_master.py  # Trading day lookups
├── operators/
│   ├── time_series.py      # Column-wise: ts_mean, ts_sum, ts_std, ts_delta, ts_delay, etc.
│   ├── cross_sectional.py  # Row-wise: rank, zscore, normalize, scale, quantile, winsorize
│   ├── arithmetic.py       # Element-wise: add, multiply, power, log, etc.
│   ├── logical.py          # Comparisons: lt, gt, eq, if_else, is_nan
│   ├── group.py            # Group ops: group_rank, group_zscore, group_neutralize
│   └── vector.py           # Vector ops: vec_avg, vec_sum
├── alpha/
│   ├── core.py             # Alpha class with operator overloading
│   └── parser.py           # alpha_eval() string DSL for GP/RL
├── types.py            # SecurityInfo dataclass
└── exceptions.py       # Custom exceptions
```

**Data flow**: `QuantDLClient.ticks()` → resolve symbols via SecurityMaster → fetch parquet from S3 (or cache) → pivot long→wide → align to trading calendar → return DataFrame

**Wide table format**: All operators expect/return DataFrames with timestamp as first column, symbols as remaining columns. Time-series ops work column-wise, cross-sectional ops work row-wise. Output rows are aligned to trading days from CalendarMaster.

**S3 bucket structure** (us-equity-datalake):
- `data/raw/ticks/daily/{security_id}/history.parquet` - OHLCV
- `data/raw/fundamental/{cik}/fundamental.parquet` - SEC filings (quarterly)
- `data/derived/features/fundamental/{cik}/ttm.parquet` - trailing twelve months
- `data/derived/features/fundamental/{cik}/metrics.parquet` - derived ratios
- `data/master/security_master.parquet` - symbol↔security_id mapping
- `data/master/calendar_master.parquet` - trading days

**Testing**: Uses `local_data_path` param to bypass S3 with local parquet files (see `conftest.py` fixtures).

## Key Patterns

- Polars-native: all data ops use Polars DataFrames/LazyFrames
- Point-in-time: symbol resolution via SecurityMaster handles ticker changes
- Concurrent fetching: ThreadPoolExecutor + asyncio for multi-symbol requests
- Operators are composable: `rank(zscore(ts_delta(prices, 20)))`
