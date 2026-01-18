# AlphaSession Reference

Unified session for alpha research with automatic S3 data fetching.

## Architecture

```
AlphaSession(client, symbols, start, end)
     │
     ├── session.close ──────► Lazy fetch ──► Alpha (cached)
     ├── session.eval("...") ─► String DSL with auto-binding ──► Alpha
     └── session.fetch("close", "volume") ► Batch ──► dict[str, Alpha]
```

**Source**: `src/quantdl/alpha/session.py`

---

## Core Components

### AlphaSession Class

**Location**: `src/quantdl/alpha/session.py:56-232`

| Attribute | Type | Description |
|-----------|------|-------------|
| `_client` | `QuantDLClient` | Data fetching backend |
| `_symbols` | `list[str]` | Symbols in session |
| `_start`, `_end` | `date` | Date range |
| `_cache` | `dict[str, Alpha]` | Cached Alpha objects |
| `_lock` | `threading.Lock` | Thread safety lock |
| `_chunk_size` | `int \| None` | Max symbols per fetch |

### DataSpec Type

**Location**: `src/quantdl/alpha/types.py:23-35`

```python
@dataclass(frozen=True, slots=True)
class DataSpec:
    source: Literal["daily", "fundamentals", "metrics"]
    field: str
```

### Field Aliases

**Location**: `src/quantdl/alpha/session.py:23-38`

```python
FIELD_ALIASES = {
    "open": DataSpec("daily", "open"),
    "high": DataSpec("daily", "high"),
    "low": DataSpec("daily", "low"),
    "close": DataSpec("daily", "close"),
    "volume": DataSpec("daily", "volume"),
    "price": DataSpec("daily", "close"),
    "revenue": DataSpec("fundamentals", "Revenue"),
    "net_income": DataSpec("fundamentals", "NetIncome"),
    "pe": DataSpec("metrics", "pe_ratio"),
    "pb": DataSpec("metrics", "pb_ratio"),
}
```

---

## Methods

### `__getattr__` - Lazy Field Access

**Location**: `src/quantdl/alpha/session.py:151-164`

| Aspect | Details |
|--------|---------|
| **Goal** | Fetch field data lazily on first attribute access |
| **Input** | Field name (e.g., `"close"`, `"volume"`) |
| **Output** | `Alpha` object wrapping wide DataFrame |

**Implementation**:
```python
def __getattr__(self, name: str) -> Alpha:
    if name.startswith("_"):
        raise AttributeError(name)
    if not self._active:
        raise SessionNotActiveError()
    with self._lock:
        if name in self._cache:
            return self._cache[name]
        alpha = self._fetch_field(name)
        self._cache[name] = alpha
        return alpha
```

**Example**:
```python
# Goal: Get closing prices as Alpha object
# Input: session.close (attribute access triggers __getattr__)
# Output: Alpha(DataFrame with timestamp + symbol columns)

with AlphaSession(client, ["AAPL", "MSFT"], "2024-01-01", "2024-12-31") as s:
    close = s.close  # First access fetches from S3
    close2 = s.close  # Second access returns cached Alpha
    assert close is close2  # Same object (cached)

    print(close.data)
    # shape: (252, 3)
    # ┌────────────┬────────┬────────┐
    # │ timestamp  │ AAPL   │ MSFT   │
    # │ date       │ f64    │ f64    │
    # ╞════════════╪════════╪════════╡
    # │ 2024-01-02 │ 185.50 │ 374.25 │
    # │ 2024-01-03 │ 184.25 │ 372.00 │
    # │ ...        │ ...    │ ...    │
    # └────────────┴────────┴────────┘
```

---

### `fetch` - Batch Fetch

**Location**: `src/quantdl/alpha/session.py:166-187`

| Aspect | Details |
|--------|---------|
| **Goal** | Fetch multiple fields in one call |
| **Input** | Variable field names (`*fields: str`) |
| **Output** | `dict[str, Alpha]` mapping field → Alpha |

**Implementation**:
```python
def fetch(self, *fields: str) -> dict[str, Alpha]:
    if not self._active:
        raise SessionNotActiveError()
    result: dict[str, Alpha] = {}
    for field in fields:
        with self._lock:
            if field in self._cache:
                result[field] = self._cache[field]
            else:
                alpha = self._fetch_field(field)
                self._cache[field] = alpha
                result[field] = alpha
    return result
```

**Example**:
```python
# Goal: Fetch close, volume, and open in one call
# Input: "close", "volume", "open"
# Output: {"close": Alpha, "volume": Alpha, "open": Alpha}

with AlphaSession(client, ["AAPL", "MSFT"], "2024-01-01", "2024-12-31") as s:
    data = s.fetch("close", "volume", "open")

    close = data["close"]
    volume = data["volume"]
    open_ = data["open"]

    # All are Alpha objects
    assert isinstance(close, Alpha)
    assert isinstance(volume, Alpha)
```

---

### `eval` - String DSL Evaluation

**Location**: `src/quantdl/alpha/session.py:189-217`

| Aspect | Details |
|--------|---------|
| **Goal** | Evaluate alpha expression string with auto-fetching |
| **Input** | Expression string (e.g., `"ops.rank(-ops.ts_delta(close, 5))"`) |
| **Output** | `Alpha` with computed result |

**Implementation**:
```python
def eval(self, expr: str) -> Alpha:
    if not self._active:
        raise SessionNotActiveError()
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as e:
        raise AlphaParseError(f"Invalid expression syntax: {e}") from e

    variables = _SessionVariableProxy(self)
    evaluator = SafeEvaluator(variables, ops)
    result = evaluator.visit(tree)

    if isinstance(result, Alpha):
        return result
    if isinstance(result, pl.DataFrame):
        return Alpha(result)
    raise AlphaParseError(...)
```

**Example**:
```python
# Goal: Compute ranked momentum signal via string expression
# Input: "ops.rank(-ops.ts_delta(close, 5))"
# Output: Alpha with cross-sectional ranks

with AlphaSession(client, ["AAPL", "MSFT", "GOOGL"], "2024-01-01", "2024-12-31") as s:
    # Variables (close) are auto-fetched from session
    alpha = s.eval("ops.rank(-ops.ts_delta(close, 5))")

    print(alpha.data)
    # shape: (252, 4)
    # ┌────────────┬──────┬──────┬───────┐
    # │ timestamp  │ AAPL │ MSFT │ GOOGL │
    # │ date       │ f64  │ f64  │ f64   │
    # ╞════════════╪══════╪══════╪═══════╡
    # │ 2024-01-08 │ 0.67 │ 0.33 │ 0.00  │
    # │ 2024-01-09 │ 0.33 │ 0.67 │ 0.00  │
    # │ ...        │ ...  │ ...  │ ...   │
    # └────────────┴──────┴──────┴───────┘
```

---

### `register` - Custom Field Registration

**Location**: `src/quantdl/alpha/session.py:219-226`

| Aspect | Details |
|--------|---------|
| **Goal** | Add custom field mappings to session |
| **Input** | Field name and `DataSpec` |
| **Output** | None (modifies session state) |

**Example**:
```python
# Goal: Register VWAP as a custom field
# Input: name="vwap", spec=DataSpec("daily", "vwap")
# Output: session.vwap now fetches daily vwap data

with AlphaSession(client, symbols, start, end) as s:
    s.register("vwap", DataSpec("daily", "vwap"))

    vwap = s.vwap  # Now works!
    signal = s.close / vwap  # Alpha arithmetic
```

---

### `_fetch_field` - Internal Fetch Logic

**Location**: `src/quantdl/alpha/session.py:115-149`

| Aspect | Details |
|--------|---------|
| **Goal** | Resolve field spec and fetch data, with optional chunking |
| **Input** | Field name |
| **Output** | `Alpha` with combined data |

**Chunking Logic** (`src/quantdl/alpha/session.py:125-149`):
```python
def _fetch_field(self, name: str) -> Alpha:
    spec = self._resolve_spec(name)

    # No chunking: fetch all at once
    if self._chunk_size is None or len(self._symbols) <= self._chunk_size:
        df = self._fetch_single_chunk(spec, self._symbols)
        return Alpha(df)

    # Chunked fetch for large universes
    chunks: list[pl.DataFrame] = []
    for i in range(0, len(self._symbols), self._chunk_size):
        chunk_symbols = self._symbols[i : i + self._chunk_size]
        chunk_df = self._fetch_single_chunk(spec, chunk_symbols)
        chunks.append(chunk_df)

    # Combine chunks by joining on timestamp
    result = chunks[0]
    for chunk in chunks[1:]:
        result = result.join(chunk, on="timestamp", how="full", coalesce=True)

    # Reorder columns
    symbol_cols = [s for s in self._symbols if s in result.columns]
    result = result.select(["timestamp", *symbol_cols])
    return Alpha(result)
```

**Example**:
```python
# Goal: Fetch 3000 symbols without OOM
# Input: chunk_size=500
# Output: Combined Alpha from 6 chunks

symbols = ["SYM" + str(i) for i in range(3000)]

with AlphaSession(client, symbols, start, end, chunk_size=500) as s:
    # Internally: fetches 500 at a time, joins results
    close = s.close  # Works without memory issues

    assert close.data.shape[1] == 3001  # timestamp + 3000 symbols
```

---

## Context Manager

**Location**: `src/quantdl/alpha/session.py:228-235`

```python
def __enter__(self) -> AlphaSession:
    self._active = True
    if self._eager and self._fields:
        self.fetch(*self._fields)
    return self

def __exit__(self, *args: object) -> None:
    self._active = False
    self._cache.clear()
```

| Aspect | Details |
|--------|---------|
| **Goal** | Manage session lifecycle, enable eager prefetch |
| **Input** | `eager=True, fields=["close", "volume"]` |
| **Output** | Fields prefetched on `__enter__` |

**Example**:
```python
# Goal: Prefetch close and volume on session start
# Input: eager=True, fields=["close", "volume"]
# Output: Both cached before any user code runs

with AlphaSession(
    client, symbols, start, end,
    eager=True, fields=["close", "volume"]
) as s:
    # Already cached - no fetch delay
    close = s.close
    volume = s.volume
```

---

## Thread Safety

**Location**: `src/quantdl/alpha/session.py:158-163`

The session uses `threading.Lock` to ensure thread-safe access:

```python
with self._lock:
    if name in self._cache:
        return self._cache[name]
    alpha = self._fetch_field(name)
    self._cache[name] = alpha
    return alpha
```

**Example**:
```python
# Goal: Safe concurrent access from multiple threads
# Input: 10 threads accessing session.close simultaneously
# Output: All threads get same cached Alpha object

import threading

with AlphaSession(client, symbols, start, end) as s:
    results = []

    def worker():
        results.append(s.close)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # All return same cached object
    assert all(r is results[0] for r in results)
```

---

## Variable Proxy

**Location**: `src/quantdl/alpha/session.py:238-261`

`_SessionVariableProxy` enables `eval()` to auto-fetch fields:

```python
class _SessionVariableProxy(dict[str, Any]):
    def __init__(self, session: AlphaSession) -> None:
        super().__init__()
        self._session = session

    def __getitem__(self, key: str) -> Any:
        if key == "ops":
            return ops
        return getattr(self._session, key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key == "ops":
            return True
        try:
            self._session._resolve_spec(key)
            return True
        except FieldNotFoundError:
            return False
```

---

## Exceptions

**Location**: `src/quantdl/alpha/validation.py:36-54`

| Exception | Trigger | Example |
|-----------|---------|---------|
| `FieldNotFoundError` | Unknown field name | `session.invalid_field` |
| `SessionNotActiveError` | Used outside context | `session.close` without `with` |

**Example**:
```python
# Goal: Handle unknown field gracefully
# Input: session.nonexistent
# Output: FieldNotFoundError

with AlphaSession(client, symbols, start, end) as s:
    try:
        _ = s.nonexistent
    except FieldNotFoundError as e:
        print(f"Unknown field: {e.field}")  # "nonexistent"

# Goal: Detect session used outside context
# Input: session.close after exiting context
# Output: SessionNotActiveError

session = AlphaSession(client, symbols, start, end)
try:
    _ = session.close  # Not in context!
except SessionNotActiveError:
    print("Must use 'with AlphaSession(...) as s:'")
```

---

## Complete Example

```python
from quantdl import QuantDLClient
from quantdl.alpha import Alpha, AlphaSession, DataSpec
import quantdl.operators as ops

client = QuantDLClient()
symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]

with AlphaSession(client, symbols, "2024-01-01", "2024-12-31") as s:
    # Lazy access
    close = s.close
    volume = s.volume

    # Compute returns
    lagged = ops.ts_delay(close.data, 1)
    returns = close / Alpha(lagged) - 1

    # Volume-weighted momentum
    momentum = ops.ts_delta(close.data, 5)
    vol_ma = ops.ts_mean(volume.data, 20)
    vol_filter = (volume.data > vol_ma).cast(pl.Float64)

    signal = Alpha(ops.rank(momentum)) * Alpha(vol_filter)

    # Or via string DSL
    signal2 = s.eval("ops.rank(-ops.ts_delta(close, 5))")

    print(signal.data)
```

---

## Test Coverage

**Location**: `tests/test_session.py`

| Test Class | Coverage |
|------------|----------|
| `TestAlphaSessionBasics` | Context manager, lazy fetch, caching |
| `TestAlphaSessionFetch` | Batch fetch, cache population |
| `TestAlphaSessionEval` | String DSL, ops namespace |
| `TestAlphaSessionRegister` | Custom field registration |
| `TestAlphaSessionEager` | Eager prefetch mode |
| `TestAlphaSessionExceptions` | Error handling |
| `TestAlphaSessionChunking` | Large universe handling |
| `TestAlphaSessionThreadSafety` | Concurrent access |
| `TestDataSpec` | Type creation, immutability, hashing |

Run tests:
```bash
uv run pytest tests/test_session.py -v
```
