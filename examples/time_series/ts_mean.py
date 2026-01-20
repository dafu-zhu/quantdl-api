"""
ts_mean - Rolling mean (moving average)

When to use:
    Use ts_mean() to smooth price data and identify trends.
    Common window sizes: 5, 10, 20, 50, 200 days.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    5-day moving average of prices
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_mean

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 5-day moving average
ma_5 = ts_mean(prices, 5)

print("ts_mean() - Rolling mean")
print("=" * 50)
print("\nOriginal prices:")
print(prices.head(7))
print("\n5-day moving average:")
print(ma_5.head(7))

# Cleanup
client.close()
