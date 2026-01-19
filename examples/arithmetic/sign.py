"""
sign - Sign function returning -1, 0, or 1

When to use:
    Use sign() to extract the direction of values.
    Returns -1 for negative, 0 for zero, 1 for positive.

Parameters:
    x: Input DataFrame

Example output:
    Sign of daily price changes
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import sign, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily change
daily_change = ts_delta(prices, 1)

# Get sign of change
sign_change = sign(daily_change)

print("sign() - Sign function returning -1, 0, or 1")
print("=" * 50)
print("\nDaily price change:")
print(daily_change.head())
print("\nSign of daily change (-1=down, 0=flat, 1=up):")
print(sign_change.head())

# Cleanup
client.close()
