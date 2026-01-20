"""
ts_rank - Percentile rank in rolling window

When to use:
    Use ts_rank() to see where current value ranks in recent history.
    Returns 0-1 (1 = highest in window, 0 = lowest).

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    Percentile rank in 5-day window
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_rank

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate percentile rank in window
percentile = ts_rank(prices, 5)

print("ts_rank() - Percentile rank in window")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\nPercentile rank in 5-day window:")
print(percentile.head(7))

# Cleanup
client.close()
