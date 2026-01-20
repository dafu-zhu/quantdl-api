"""
ts_count_nans - Count null values in rolling window

When to use:
    Use ts_count_nans() to check data quality over a window.
    Useful for filtering stocks with missing data.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    Count of NaN values in 10-day window
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_count_nans, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily change (first row is NaN)
daily_change = ts_delta(prices, 1)

# Count NaN in rolling window
nan_count = ts_count_nans(daily_change, 5)

print("ts_count_nans() - Count nulls in window")
print("=" * 50)
print("\nDaily change (first rows have NaN):")
print(daily_change.head(5))
print("\nNaN count in 10-day window:")
print(nan_count.head(15))

# Cleanup
client.close()
