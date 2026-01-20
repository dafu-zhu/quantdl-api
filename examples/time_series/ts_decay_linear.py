"""
ts_decay_linear - Linear decay weighted average

When to use:
    Use ts_decay_linear() for recent-weighted moving averages.
    Weights: [1, 2, 3, ..., d] normalized, so recent data has more impact.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    5-day linear decay weighted average
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_decay_linear

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate linear decay weighted average
decay_avg = ts_decay_linear(prices, 5)

print("ts_decay_linear() - Linear decay weighted average")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\n5-day linear decay weighted average:")
print(decay_avg.head(7))

# Cleanup
client.close()
