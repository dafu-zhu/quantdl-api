"""
ne - Not equal comparison (!=)

When to use:
    Use ne() to check if values are different.
    Works with scalars or DataFrames.

Parameters:
    x: First DataFrame
    y: Second DataFrame or scalar

Example output:
    Boolean mask where sign of daily change is not zero (non-flat day)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ne, sign, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate sign of daily change
daily_change = ts_delta(prices, 1)
change_sign = sign(daily_change)

# Check for non-flat days (sign != 0)
not_flat = ne(change_sign, 0)

print("ne() - Not equal comparison (!=)")
print("=" * 50)
print("\nSign of daily change:")
print(change_sign.head())
print("\nNot flat day (sign != 0):")
print(not_flat.head())

# Cleanup
client.close()
