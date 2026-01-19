"""
ts_backfill - Fill nulls with last valid value

When to use:
    Use ts_backfill() to forward-fill missing data.
    Propagates last known value into NaN gaps.

Parameters:
    x: Input DataFrame
    d: Maximum lookback for fill

Example output:
    Forward-filled data
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_backfill, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create data with NaN (first row from delta)
daily_change = ts_delta(prices, 1)
sparse = daily_change.head(10)

# Backfill the NaN values
filled = ts_backfill(sparse, 5)

print("ts_backfill() - Fill nulls with last valid")
print("=" * 50)
print("\nOriginal (with NaN in first row):")
print(sparse.head(3))
print("\nAfter backfill:")
print(filled.head(3))

# Cleanup
client.close()
