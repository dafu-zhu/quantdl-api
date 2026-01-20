"""
ts_scale - Rolling min-max scale [0, 1]

When to use:
    Use ts_scale() to normalize values to [0, 1] range over time.
    Shows where current price is relative to recent range.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    5-day scaled price (0 = at low, 1 = at high)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_scale

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 5-day scaled price
scaled_price = ts_scale(prices, 5)

print("ts_scale() - Rolling min-max scale [0, 1]")
print("=" * 50)
print("\nPrices:")
print(prices.head(6))
print("\n5-day scaled price [0=low, 1=high] (partial windows, min 2):")
print(scaled_price.head(7))

# Cleanup
client.close()
