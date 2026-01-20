"""
ts_min - Rolling minimum

When to use:
    Use ts_min() to find the lowest value in a rolling window.
    Useful for support levels, drawdown analysis, etc.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    5-day rolling low
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_min

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 5-day rolling low
rolling_low = ts_min(prices, 5)

print("ts_min() - Rolling minimum")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\n5-day rolling low:")
print(rolling_low.head(7))

# Cleanup
client.close()
