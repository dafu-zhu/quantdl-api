"""
eq - Equality comparison (==)

When to use:
    Use eq() to check if values are equal.
    Works with scalars or DataFrames.

Parameters:
    x: First DataFrame
    y: Second DataFrame or scalar

Example output:
    Boolean mask where sign of daily change equals 1 (up day)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import eq, sign, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate sign of daily change
daily_change = ts_delta(prices, 1)
change_sign = sign(daily_change)

# Check for up days (sign == 1)
is_up_day = eq(change_sign, 1)

print("eq() - Equality comparison (==)")
print("=" * 50)
print("\nSign of daily change:")
print(change_sign.head())
print("\nIs up day (sign == 1):")
print(is_up_day.head())

# Cleanup
client.close()
