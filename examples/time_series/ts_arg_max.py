"""
ts_arg_max - Days since rolling window maximum

When to use:
    Use ts_arg_max() to find when the high occurred in the window.
    Returns 0 if today is the high, d-1 if high was at start of window.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    Days since 4-day high
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_arg_max

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Find days since rolling high
days_since_high = ts_arg_max(prices, 4)

print("ts_arg_max() - Days since window maximum")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\nDays since 4-day high (0 = today is high):")
print(days_since_high.head(7))

# Cleanup
client.close()
