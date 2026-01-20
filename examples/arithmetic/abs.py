"""
abs - Absolute value of each element

When to use:
    Use abs() to get the magnitude of values, ignoring sign.
    Useful for measuring the size of price changes regardless of direction.

Parameters:
    x: Input DataFrame

Example output:
    Absolute value of daily price changes
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import abs as ops_abs
from quantdl.operators import ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily price change
daily_change = ts_delta(prices, 1)

# Apply abs to get magnitude of change
abs_change = ops_abs(daily_change)

print("abs() - Absolute value")
print("=" * 50)
print("\nDaily prices:")
print(prices.head())
print("\nOriginal daily change (can be negative):")
print(daily_change.head())
print("\nAbsolute daily change (always positive):")
print(abs_change.head())

# Cleanup
client.close()
