# Task: QuantDL API - S3 Data Fetching PyPI Package

> **Template Version**: 1.1
> **Created**: 2026-01-16
> **Refined by**: Task Refiner (Interactive Session)

---

## Goal

Build a PyPI package that fetches financial data from AWS S3 bucket "us-equity-datalake", returns wide table format (row=date, col=symbol, val=field) for alpha research, with point-in-time data resolution, local disk caching, and core alpha operators.

---

## Context

### Current State
- Starting fresh - no existing codebase
- AWS S3 bucket "us-equity-datalake" contains organized financial data:
  - `/data/master/security_master.parquet` - security identification mapping
  - `/data/raw/fundamental/{cik}/fundamental.parquet` - raw fundamental data
  - `/data/derived/features/fundamental/{cik}/` - ttm.parquet, metrics.parquet
  - `/data/raw/ticks/daily/{security_id}/` - daily price data (history.parquet + monthly partitions)
  - `/data/symbols/{year}/{month:02d}/top3000.txt` - universe file
- Data stored as parquet files in long table format

### Problem
- Need unified API to fetch diverse financial data sources
- Must handle point-in-time lookups (symbol -> security_id -> data)
- Parquet files in long format need transformation to wide tables for alpha research
- Must minimize S3 API calls (3000 securities = 3000 potential requests)
- Balance S3 costs, memory usage, and latency

### Key Design Decisions
1. **Universe-based fetching**: Primary API fetches entire universe at once, returns wide table
2. **Local disk cache**: Cache parquet files at `~/.quantdl/cache/` with configurable TTL (default 24h) and size limit (default 10GB, LRU eviction)
3. **Direct methods API**: Simple function calls like `client.daily(universe, field, start, end)`
4. **Wide table output**: All APIs return wide tables (row=date, col=symbol, val=field)
5. **Polars-native**: All operations use polars, no pandas dependency

### Constraints
- **Python**: 3.12+
- **Memory**: 16GB limit
- **Latency**: Interactive (<5s) for typical queries (with warm cache)
- **Auth**: Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
- **Dependencies**: polars (required), boto3, s3fs, pyarrow, aiohttp (allowed)
- **No pandas**: Pandas compatibility NOT required
- **Distribution**: Public PyPI registry
- **Minute data**: Out of MVP scope (deferred)

---

## Success Criteria (Global)

- [ ] All tests pass (`uv run pytest`)
- [ ] Linting clean (`uv run ruff check .`)
- [ ] Type checking passes (`uv run mypy src/`)
- [ ] Test coverage >= 80%
- [ ] Package installable via `pip install quantdl`
- [ ] Example notebook demonstrates full workflow
- [ ] README documents all public APIs
- [ ] Memory usage stays under 16GB for typical workloads
- [ ] Query latency <5s for single field wide table (warm cache)

---

## Safety Bounds

**Maximum iterations per task**: 30
**Maximum cost**: $10
**Maximum time**: 2 hours

> These bounds prevent runaway execution. Orchestrator will pause and ask for approval if any limit is approached.

---

## Phases & Tasks

### Phase 1: Project Foundation & Core Infrastructure

**Goal**: Establish project structure, storage client, security_master lookup, and caching layer

#### Task 1.1: Initialize Python package structure

**Description**: Create package structure with pyproject.toml, src layout, test scaffolding using uv

**Success Criteria**:
- [ ] `pyproject.toml` with metadata, dependencies (polars, boto3, s3fs, pyarrow, aiohttp)
- [ ] `pyproject.toml` includes ruff and mypy configuration sections
- [ ] `src/quantdl/` package with `__init__.py` and `py.typed` marker
- [ ] `tests/` directory with `conftest.py`
- [ ] `.github/workflows/ci.yml` CI stub (lint, type check, test)
- [ ] `.gitignore` for Python projects
- [ ] `.python-version` file specifying 3.12
- [ ] `uv sync` installs all dependencies
- [ ] `uv run pytest` runs (even with no tests)
- [ ] `uv run ruff check .` passes
- [ ] `uv run mypy src/` passes

**Agents Needed**: None

**Complexity**: Simple

**Dependencies**: None

---

#### Task 1.2: Implement storage client wrapper

**Description**: Create authenticated storage client with connection pooling, retry logic, and async support

**Success Criteria**:
- [ ] `QuantDLClient` class as main entry point
- [ ] Reads credentials from environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
- [ ] Bucket name configurable via env var `QUANTDL_BUCKET` (default: "us-equity-datalake")
- [ ] Connection pooling configured for efficiency
- [ ] Retry logic for transient failures (3 retries, exponential backoff)
- [ ] Internal `_read_parquet(path) -> pl.DataFrame` method (sync)
- [ ] Internal `_read_parquet_async(path) -> pl.DataFrame` method (async)
- [ ] Internal `_list_objects(prefix) -> list[str]` method
- [ ] Unit tests with mocked S3 (moto library)
- [ ] Integration test fixture for real S3 (skipped by default via pytest marker)

**Agents Needed**:
- `fintech-engineer` (S3 best practices for financial data)

**Complexity**: Medium

**Dependencies**: Task 1.1

---

#### Task 1.3: Implement security_master lookup

**Description**: Build point-in-time security resolution from symbol to security_id

**Success Criteria**:
- [ ] `SecurityMaster` class (internal, used by QuantDLClient)
- [ ] Loads and caches security_master.parquet lazily on first access
- [ ] `resolve(symbol: str, as_of_date: date) -> SecurityInfo | None`
- [ ] `SecurityInfo` is a dataclass with fields: security_id, cik, permno, symbol, company
- [ ] Returns `None` for missing symbols (not exception)
- [ ] Handles symbol changes correctly (FB -> META) via date range matching
- [ ] `resolve_batch(symbols: list[str], as_of_date: date) -> dict[str, SecurityInfo | None]` for efficiency
- [ ] Memory-efficient: single load, session lifetime
- [ ] 95%+ test coverage including edge cases: missing symbol, symbol change, overlapping date ranges

**Agents Needed**:
- `quant-analyst` (point-in-time data semantics)
- `fintech-engineer` (security identifier handling)

**Complexity**: Medium

**Dependencies**: Task 1.2

---

#### Task 1.4: Implement local disk cache

**Description**: Build caching layer to minimize S3 requests

**Success Criteria**:
- [ ] Cache directory at `~/.quantdl/cache/` (configurable via env var `QUANTDL_CACHE_DIR`)
- [ ] Time-based TTL: default 24h, configurable via `QuantDLClient(cache_ttl_hours=24)`
- [ ] Size limit: default 10GB, configurable via `QuantDLClient(cache_max_gb=10)`
- [ ] LRU eviction when cache exceeds size limit
- [ ] Cache key based on S3 path
- [ ] `_get_cached_or_fetch(path) -> pl.DataFrame` method checks cache first
- [ ] `clear_cache()` method to manually clear cache
- [ ] Cache metadata stored in `~/.quantdl/cache/metadata.json`
- [ ] Thread-safe cache operations
- [ ] Unit tests for cache hit, miss, expiry, eviction

**Agents Needed**:
- `build-engineer` (caching patterns)

**Complexity**: Medium

**Dependencies**: Task 1.2

---

### Phase 2: Daily Ticks API

**Goal**: Implement daily price data retrieval with universe-based fetching and wide table output

#### Task 2.1: Implement daily ticks fetcher (internal)

**Description**: Internal method to fetch daily OHLCV data for a single security

**Success Criteria**:
- [ ] Internal `_get_daily_ticks(security_id: str, start: date, end: date) -> pl.DataFrame`
- [ ] Efficiently reads from history.parquet and/or current year monthly partitions based on date range
- [ ] Returns long table with columns: timestamp, open, high, low, close, volume
- [ ] Returns empty DataFrame (with correct schema) if security not found
- [ ] Uses cache layer from Task 1.4
- [ ] Predicate pushdown for date filtering where possible

**Agents Needed**:
- `fintech-engineer` (time-series data handling)

**Complexity**: Medium

**Dependencies**: Task 1.4

---

#### Task 2.2: Implement universe loader

**Description**: Load and resolve universe of symbols

**Success Criteria**:
- [ ] `_load_universe(universe: str | list[str], as_of: date) -> list[SecurityInfo]`
- [ ] If `universe='top3000'`, loads from `/data/symbols/{year}/{month:02d}/top3000.txt`
- [ ] If `universe` is list of symbols, resolves each via SecurityMaster
- [ ] Filters out symbols that resolve to None (logs warning)
- [ ] Returns list of SecurityInfo for valid symbols
- [ ] Caches universe file with same TTL as other data

**Agents Needed**: None

**Complexity**: Simple

**Dependencies**: Task 1.3

---

#### Task 2.3: Implement daily wide table API

**Description**: Main user-facing API for daily price data

**Success Criteria**:
- [ ] `client.daily(universe: str | list[str], field: str, start: date, end: date, as_of: date | None = None) -> pl.DataFrame`
- [ ] `field` is one of: 'open', 'high', 'low', 'close', 'volume'
- [ ] `as_of` defaults to `end` date for point-in-time resolution
- [ ] Returns wide table: rows = dates, columns = symbols, values = field
- [ ] Async parallel fetching for multiple securities (configurable concurrency, default 10)
- [ ] Progress logging for long-running fetches (>10 securities)
- [ ] Handles partial failures gracefully: logs warning, excludes failed securities from result
- [ ] Latency <5s for 100 securities, 1 year, warm cache
- [ ] Clear error messages for invalid field names

**Agents Needed**:
- `fintech-engineer` (batch processing patterns)

**Complexity**: High

**Dependencies**: Task 2.1, Task 2.2

---

### Phase 3: Fundamentals API

**Goal**: Implement fundamental data retrieval (raw, TTM, derived metrics) with wide table output

#### Task 3.1: Implement fundamentals fetcher (internal)

**Description**: Internal method to fetch fundamental data for a single CIK

**Success Criteria**:
- [ ] Internal `_get_fundamentals(cik: str, ttm: bool = False) -> pl.DataFrame`
- [ ] If `ttm=False`, reads from `/data/raw/fundamental/{cik}/fundamental.parquet`
- [ ] If `ttm=True`, reads from `/data/derived/features/fundamental/{cik}/ttm.parquet`
- [ ] Returns long table with columns: symbol, as_of_date, accn, form, concept, value, start, end, frame
- [ ] Returns empty DataFrame (with correct schema) if CIK not found
- [ ] Uses cache layer

**Agents Needed**:
- `quant-analyst` (fundamental data semantics)

**Complexity**: Medium

**Dependencies**: Task 1.4

---

#### Task 3.2: Implement fundamentals wide table API

**Description**: Main user-facing API for fundamental data

**Success Criteria**:
- [ ] `client.fundamentals(universe: str | list[str], concepts: str | list[str], start: date, end: date, ttm: bool = False, as_of: date | None = None) -> pl.DataFrame | dict[str, pl.DataFrame]`
- [ ] If single concept string, returns single wide table
- [ ] If list of concepts, returns dict keyed by concept name
- [ ] Wide table: rows = dates (daily calendar), columns = symbols, values = concept value
- [ ] Forward-fill from last filing date to create daily alignment
- [ ] If `ttm=True` requested for non-income-statement concept, raises `ValueError` with clear message
- [ ] Document which concepts support TTM (income statement items only)
- [ ] Async parallel fetching by CIK
- [ ] `as_of` defaults to `end` date

**Agents Needed**:
- `quant-analyst` (TTM calculation semantics, forward-fill logic)

**Complexity**: High

**Dependencies**: Task 3.1, Task 2.2

---

#### Task 3.3: Implement derived metrics fetcher (internal)

**Description**: Internal method to fetch pre-calculated metrics for a single CIK

**Success Criteria**:
- [ ] Internal `_get_metrics(cik: str) -> pl.DataFrame`
- [ ] Reads from `/data/derived/features/fundamental/{cik}/metrics.parquet`
- [ ] Returns long table with columns: symbol, as_of_date, metric, value
- [ ] Returns empty DataFrame (with correct schema) if CIK not found

**Agents Needed**: None

**Complexity**: Simple

**Dependencies**: Task 1.4

---

#### Task 3.4: Implement metrics wide table API

**Description**: User-facing API for derived financial metrics

**Success Criteria**:
- [ ] `client.metrics(universe: str | list[str], metrics: str | list[str], start: date, end: date, as_of: date | None = None) -> pl.DataFrame | dict[str, pl.DataFrame]`
- [ ] Same output pattern as fundamentals (single -> DataFrame, list -> dict)
- [ ] Wide table with forward-fill alignment
- [ ] `client.list_metrics() -> list[str]` to discover available metrics
- [ ] Architecture supports future extension to ticks-derived metrics (e.g., moving averages, volatility)
- [ ] Extension point: registry pattern for metric types (fundamental-derived, future: ticks-derived)

**Agents Needed**:
- `quant-analyst` (financial metrics)

**Complexity**: Medium

**Dependencies**: Task 3.3, Task 2.2

---

### Phase 4: Alpha Operators

**Goal**: Implement core time-series and cross-sectional operators for alpha research

#### Task 4.1: Implement time-series operators

**Description**: Core time-series operators that operate along the time dimension of wide tables

**Success Criteria**:
- [ ] All operators accept wide table (pl.DataFrame) and return wide table of same shape
- [ ] `ts_mean(x: pl.DataFrame, d: int) -> pl.DataFrame` - rolling mean over d days
- [ ] `ts_std_dev(x: pl.DataFrame, d: int) -> pl.DataFrame` - rolling std dev over d days
- [ ] `ts_sum(x: pl.DataFrame, d: int) -> pl.DataFrame` - rolling sum over d days
- [ ] `ts_delay(x: pl.DataFrame, d: int) -> pl.DataFrame` - value d days ago
- [ ] `ts_delta(x: pl.DataFrame, d: int) -> pl.DataFrame` - x - ts_delay(x, d)
- [ ] `ts_rank(x: pl.DataFrame, d: int) -> pl.DataFrame` - rank of current value in last d days (0-1 scale)
- [ ] `ts_zscore(x: pl.DataFrame, d: int) -> pl.DataFrame` - z-score over last d days
- [ ] All operators handle NaN values correctly (skip in calculations)
- [ ] All operators preserve DataFrame index (date column)
- [ ] Unit tests with edge cases: all NaN column, single row, d > available history

**Agents Needed**:
- `quant-analyst` (time-series operator semantics)

**Complexity**: Medium

**Dependencies**: Task 2.3

---

#### Task 4.2: Implement cross-sectional operators

**Description**: Cross-sectional operators that operate across securities at each point in time

**Success Criteria**:
- [ ] All operators accept wide table and return wide table of same shape
- [ ] `rank(x: pl.DataFrame, rate: float = 2) -> pl.DataFrame` - rank across securities at each date (0-1 scale)
- [ ] `zscore(x: pl.DataFrame) -> pl.DataFrame` - cross-sectional z-score at each date
- [ ] `normalize(x: pl.DataFrame, use_std: bool = False) -> pl.DataFrame` - demean (optionally divide by std)
- [ ] `winsorize(x: pl.DataFrame, std: float = 4) -> pl.DataFrame` - clip outliers beyond std limits
- [ ] All operators handle NaN values correctly (exclude from calculation)
- [ ] Unit tests with edge cases: all NaN row, single security, extreme outliers

**Agents Needed**:
- `quant-analyst` (cross-sectional operator semantics)

**Complexity**: Medium

**Dependencies**: Task 4.1

---

#### Task 4.3: Operator module organization

**Description**: Organize operators into clean public API

**Success Criteria**:
- [ ] Operators importable as `from quantdl.operators import rank, ts_mean, ...`
- [ ] Alternative import: `from quantdl import operators as ops; ops.rank(...)`
- [ ] `quantdl.operators.__all__` lists all public operators
- [ ] Each operator has docstring with: description, parameters, returns, example
- [ ] Type hints for all operator functions
- [ ] Operators are composable: `rank(ts_mean(close, 20))`

**Agents Needed**: None

**Complexity**: Simple

**Dependencies**: Task 4.2

---

### Phase 5: PyPI Packaging & Documentation

**Goal**: Prepare package for public PyPI release with comprehensive documentation

#### Task 5.1: Finalize package metadata and build

**Description**: Complete pyproject.toml, versioning, and build configuration

**Success Criteria**:
- [ ] Version 0.1.0 set in pyproject.toml
- [ ] All dependencies pinned with version ranges (e.g., `polars>=0.20,<1.0`)
- [ ] `uv build` produces wheel and sdist
- [ ] Package installs cleanly in fresh environment: `pip install dist/quantdl-0.1.0-py3-none-any.whl`
- [ ] Verify imports work: `from quantdl import QuantDLClient`
- [ ] Verify operators work: `from quantdl.operators import rank`

**Agents Needed**:
- `build-engineer` (Python packaging best practices)

**Complexity**: Simple

**Dependencies**: Task 4.3

---

#### Task 5.2: Write comprehensive README

**Description**: Document installation, quickstart, and API overview

**Success Criteria**:
- [ ] Installation instructions (pip from PyPI, from source with uv)
- [ ] AWS credentials setup guide (env vars, boto3 config)
- [ ] Quickstart example (10 lines to first data):
  ```python
  from quantdl import QuantDLClient
  client = QuantDLClient()
  close = client.daily('top3000', 'close', '2024-01-01', '2024-12-31')
  ```
- [ ] API overview with common use cases:
  - Fetch daily prices for universe
  - Fetch fundamentals with TTM
  - Apply alpha operators
- [ ] Performance tips (caching, batch fetching)
- [ ] Cache management (location, clearing, configuration)
- [ ] Available operators list with brief descriptions
- [ ] Contributing guidelines

**Agents Needed**:
- `documentation-engineer` (README best practices)
- `api-documenter` (API documentation)

**Complexity**: Medium

**Dependencies**: Task 5.1

---

#### Task 5.3: Generate API reference documentation

**Description**: Create comprehensive API docs from docstrings

**Success Criteria**:
- [ ] All public classes and methods have docstrings (Google style)
- [ ] Docstrings include: description, Args, Returns, Raises, Example
- [ ] `QuantDLClient` docstring documents all public methods
- [ ] Each operator docstring includes usage example
- [ ] mkdocs configuration in `mkdocs.yml`
- [ ] `mkdocs build` generates static site in `site/`
- [ ] API reference navigable and searchable

**Agents Needed**:
- `api-documenter` (API documentation)

**Complexity**: Medium

**Dependencies**: Task 5.2

---

#### Task 5.4: Create example notebook

**Description**: Jupyter notebook demonstrating full workflow

**Success Criteria**:
- [ ] Notebook in `examples/quickstart.ipynb`
- [ ] Demonstrates:
  - Client initialization
  - Fetching daily prices for top3000
  - Fetching fundamentals (raw and TTM)
  - Fetching derived metrics
  - Applying TS operators (ts_mean, ts_zscore)
  - Applying CS operators (rank, zscore)
  - Combining data sources for simple alpha
- [ ] Shows cache management (checking size, clearing)
- [ ] Includes markdown explanations between code cells
- [ ] Can run with live S3 access (requires credentials)

**Agents Needed**:
- `quant-analyst` (alpha research workflow)

**Complexity**: Medium

**Dependencies**: Task 5.3

---

#### Task 5.5: Final test suite and CI

**Description**: Ensure comprehensive tests and CI pipeline

**Success Criteria**:
- [ ] Test coverage >= 80% (`uv run pytest --cov=src/quantdl --cov-report=term-missing`)
- [ ] Unit tests for all modules:
  - `test_client.py` - QuantDLClient methods
  - `test_security_master.py` - security resolution
  - `test_cache.py` - caching layer
  - `test_daily.py` - daily ticks API
  - `test_fundamentals.py` - fundamentals API
  - `test_metrics.py` - metrics API
  - `test_operators.py` - all operators
- [ ] Integration tests with S3 mocking (moto)
- [ ] GitHub Actions CI in `.github/workflows/ci.yml`:
  - Runs on push to main and PRs
  - Python 3.12
  - Steps: install deps, lint (ruff), type check (mypy), test (pytest)
- [ ] All checks pass on main branch

**Agents Needed**:
- `build-engineer` (CI configuration)

**Complexity**: Medium

**Dependencies**: Task 5.4

---

## Supervisor Checks

**Enable Supervisor**: Yes

**Check Frequency**:
- [x] Pre-task (validate approach before starting)
- [x] Post-task (verify success criteria met)
- [ ] On-demand

**Check For**:
- [x] Architecture violations (maintain clean layered design)
- [x] Cross-task dependency conflicts
- [x] API contract changes (breaking changes to public API)
- [x] Pattern inconsistencies (error handling, logging)
- [x] Memory/performance concerns
- [x] Cache correctness (TTL, eviction)

**Auto-Fix Minor Issues**: Yes

---

## Outer Ralph Loop

**Enable Outer Ralph**: Yes

**Success Criteria**:
- [ ] No code smells (cyclomatic complexity < 10)
- [ ] All public functions have docstrings with examples
- [ ] Consistent patterns (error handling, type hints, logging)
- [ ] No hardcoded S3 paths or credentials
- [ ] No hardcoded cache paths (configurable)
- [ ] Test coverage >= 80%

**Focus Areas**:
- Code quality
- Test coverage
- Type safety
- Documentation
- Performance
- Cache correctness

**Maximum Iterations**: 10

---

## Delegation Strategy

### Proactive Delegations

**Architect**:
- [ ] Consult before Phase 1 (validate overall package architecture)
- [ ] Consult before Task 1.4 (cache design review)

**Security Analyst**:
- [ ] Review credential handling in Task 1.2

**Code Reviewer**:
- [x] Review after Phase 3 (API design review)
- [x] Final review after Phase 5

### Reactive Delegations

- [x] After 2+ failed iterations -> Architect
- [x] Memory/performance issue detected -> Architect
- [x] Complex design decision needed -> Architect
- [x] Cache-related bugs -> Build Engineer

---

## Verification Requirements

### Automated Checks

**Tests**:
```bash
uv run pytest --cov=src/quantdl --cov-report=term-missing
# Must exit 0, coverage >= 80%
```

**Linting**:
```bash
uv run ruff check .
# Must exit 0
```

**Type Checking**:
```bash
uv run mypy src/
# Must exit 0
```

**Build**:
```bash
uv build
# Must produce wheel and sdist
```

### Manual Checks

- [ ] Package installs in fresh virtualenv
- [ ] README quickstart example works
- [ ] Example notebook runs without errors
- [ ] Cache behavior correct (hit, miss, expiry, eviction)
- [ ] Memory usage reasonable for top3000, 1-year daily data fetch

---

## Rollback Plan

**If task fails after max iterations**:
1. Preserve all code in feature branch (do not delete)
2. Document failure reason in GitHub issue
3. Preserve learnings.jsonl for pattern extraction
4. Identify specific blocker for next attempt

---

## Learning Objectives

**Patterns to discover**:
- [ ] Optimal S3 access patterns for financial parquet data
- [ ] Memory-efficient polars transformations for wide tables
- [ ] Point-in-time security resolution patterns
- [ ] Effective caching strategies for time-series data

**Antipatterns to avoid**:
- [ ] Loading entire datasets into memory
- [ ] Sequential S3 fetches (use parallel async)
- [ ] Hardcoded paths or credentials
- [ ] Missing type hints on public API
- [ ] Cache without size limits

---

## Notes

### S3 Data Structure Reference

```
/data
    /derived/features/fundamental/{cik}/
        /metrics.parquet
        /ttm.parquet
    /master
        /security_master.parquet
    /raw
        /fundamental/{cik}/fundamental.parquet
        /ticks
            /daily/{security_id}/
                /history.parquet
                /{current_year}/{month:02d}/ticks.parquet
            /minute/{security_id}/{month:02d}/{day:02d}/ticks.parquet  # OUT OF SCOPE
    /symbols/{year}/{month:02d}/top3000.txt
```

### Key Design Decisions

1. **Polars over Pandas**: Performance for financial data operations
2. **Universe-based fetching**: Minimize S3 requests by fetching entire universe
3. **Local disk cache**: Reduce S3 costs with TTL-based cache
4. **Session-based state**: Client maintains credentials and cache, methods are stateless
5. **Point-in-time by default**: All lookups use as_of date semantics
6. **Wide table output**: Optimized for alpha operator patterns
7. **Direct methods API**: Simple `client.daily(...)` calls, no method chaining
8. **Async parallel fetching**: Concurrent S3 requests for performance

### API Summary

```python
from quantdl import QuantDLClient
from quantdl.operators import rank, zscore, ts_mean, ts_delay

# Initialize client
client = QuantDLClient()  # reads AWS creds from env

# Daily prices
close = client.daily('top3000', 'close', '2024-01-01', '2024-12-31')
volume = client.daily('top3000', 'volume', '2024-01-01', '2024-12-31')

# Fundamentals (raw and TTM)
revenue = client.fundamentals('top3000', 'revenue', '2024-01-01', '2024-12-31', ttm=True)

# Derived metrics
pe = client.metrics('top3000', 'pe_ratio', '2024-01-01', '2024-12-31')

# Apply operators
alpha = rank(ts_mean(close, 20) - ts_delay(close, 1))
```

### MVP Operators

**Time-Series** (7):
- `ts_mean(x, d)` - rolling mean
- `ts_std_dev(x, d)` - rolling std
- `ts_sum(x, d)` - rolling sum
- `ts_delay(x, d)` - lag
- `ts_delta(x, d)` - difference
- `ts_rank(x, d)` - rolling rank
- `ts_zscore(x, d)` - rolling z-score

**Cross-Sectional** (4):
- `rank(x)` - cross-sectional rank
- `zscore(x)` - cross-sectional z-score
- `normalize(x)` - demean
- `winsorize(x)` - clip outliers

### Out of Scope (Future)

- Minute-level tick data
- Group operators (require sector/industry data)
- Custom metric registration
- Ticks-derived metrics (MA, volatility as stored metrics)
- CLI interface

### Helpful Resources

- Polars documentation: https://pola.rs/
- boto3 S3 guide: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html
- PyPI packaging guide: https://packaging.python.org/
