"""
ts_zscore - Rolling z-score (standardization over time)

When to use:
    Use ts_zscore() to normalize values relative to recent history.
    Helps identify when current value is unusually high or low.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    5-day rolling z-score of prices
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_zscore

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 5-day rolling z-score
price_zscore = ts_zscore(prices, 5)

print("ts_zscore() - Rolling z-score")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\n5-day rolling z-score (partial windows, min 2 for std):")
print(price_zscore.head(7))

# Cleanup
client.close()
