"""
ts_max - Rolling maximum

When to use:
    Use ts_max() to find the highest value in a rolling window.
    Useful for resistance levels, new highs analysis, etc.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    5-day rolling high
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_max

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 5-day rolling high
rolling_high = ts_max(prices, 5)

print("ts_max() - Rolling maximum")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\n5-day rolling high:")
print(rolling_high.head(7))

# Cleanup
client.close()
