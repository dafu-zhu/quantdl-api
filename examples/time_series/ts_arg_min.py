"""
ts_arg_min - Days since rolling window minimum

When to use:
    Use ts_arg_min() to find when the low occurred in the window.
    Returns 0 if today is the low, d-1 if low was at start of window.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    Days since 4-day low
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_arg_min

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Find days since rolling low
days_since_low = ts_arg_min(prices, 4)

print("ts_arg_min() - Days since window minimum")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\nDays since 4-day low (0 = today is low):")
print(days_since_low.head(7))

# Cleanup
client.close()
