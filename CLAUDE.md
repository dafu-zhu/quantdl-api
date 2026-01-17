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
│   ├── s3.py           # S3StorageBackend: Polars native scan_parquet, supports local mode
│   └── cache.py        # DiskCache: LRU disk cache with TTL
├── data/
│   ├── security_master.py  # Symbol→security_id resolution (point-in-time)
│   └── calendar_master.py  # Trading day lookups
├── operators/
│   ├── time_series.py      # Column-wise: ts_mean, ts_sum, ts_std, ts_delta, ts_delay, ts_corr, ts_regression, etc.
│   └── cross_sectional.py  # Row-wise: rank, zscore, normalize, scale, quantile, winsorize
├── types.py            # SecurityInfo dataclass
└── exceptions.py       # Custom exceptions
```

**Data flow**: `QuantDLClient.daily()` → resolve symbols via SecurityMaster → fetch parquet from S3 (or cache) → pivot long→wide → align to trading calendar → return DataFrame

**Wide table format**: All operators expect/return DataFrames with timestamp as first column, symbols as remaining columns. Time-series ops work column-wise, cross-sectional ops work row-wise. Output rows are aligned to trading days from CalendarMaster.

**S3 bucket structure** (us-equity-datalake):
- `data/raw/ticks/daily/{security_id}/history.parquet` - OHLCV
- `data/raw/fundamental/{cik}/fundamental.parquet` - SEC filings
- `data/derived/features/fundamental/{cik}/metrics.parquet` - derived ratios
- `data/master/security_master.parquet` - symbol↔security_id mapping
- `data/master/calendar_master.parquet` - trading days

**Testing**: Uses `local_data_path` param to bypass S3 with local parquet files (see `conftest.py` fixtures).

## Key Patterns

- Polars-native: all data ops use Polars DataFrames/LazyFrames
- Point-in-time: symbol resolution via SecurityMaster handles ticker changes
- Concurrent fetching: ThreadPoolExecutor + asyncio for multi-symbol requests
- Operators are composable: `rank(zscore(ts_delta(prices, 20)))`
