"""
is_nan - Detect NaN/null values

When to use:
    Use is_nan() to identify missing values.
    Returns True for NaN/null, False otherwise.

Parameters:
    x: Input DataFrame

Example output:
    Boolean mask showing where data is missing
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import is_nan, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily returns (first row will be NaN)
daily_change = ts_delta(prices, 1)

# Detect NaN values
has_nan = is_nan(daily_change)

print("is_nan() - Detect NaN/null values")
print("=" * 50)
print("\nDaily change (first row is NaN from delta):")
print(daily_change.head())
print("\nIs NaN (True where missing):")
print(has_nan.head())

# Cleanup
client.close()
