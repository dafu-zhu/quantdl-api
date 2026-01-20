"""
ts_backfill - Fill nulls with last valid value

When to use:
    Use ts_backfill() to forward-fill missing data.
    Propagates last known value into NaN gaps.

Parameters:
    x: Input DataFrame
    d: Maximum lookback for fill (max consecutive nulls to fill)

Example output:
    Forward-filled data where NaN gaps are filled with prior values
"""
import polars as pl
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_backfill

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create sparse data with NaN gaps in the middle (simulating missing data)
sparse = prices.head(7).with_columns(
    pl.when(pl.col("IBM").is_not_null() & (pl.int_range(pl.len()) >= 2) & (pl.int_range(pl.len()) <= 4))
    .then(pl.lit(None))
    .otherwise(pl.col("IBM"))
    .alias("IBM")
)

# Backfill the NaN values (fill up to 5 consecutive nulls)
filled = ts_backfill(sparse, 5)

print("ts_backfill() - Fill nulls with last valid")
print("=" * 50)
print("\nOriginal (with NaN gaps in rows 2-4):")
print(sparse.select(["timestamp", "IBM"]).head(7))
print("\nAfter backfill (NaN filled with prior value):")
print(filled.select(["timestamp", "IBM"]).head(7))

# Cleanup
client.close()
