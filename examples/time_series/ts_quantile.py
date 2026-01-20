"""
ts_quantile - Transform rank to Gaussian via inverse CDF

When to use:
    Use ts_quantile() to convert time-series rank to Gaussian distribution.
    Useful for normalizing signals to have standard Gaussian properties.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    Gaussian quantile transform of prices
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_quantile

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate Gaussian quantile transform
gaussian_rank = ts_quantile(prices, 5)

print("ts_quantile() - Gaussian quantile transform")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\nGaussian quantile transform:")
print(gaussian_rank.head(7))

# Cleanup
client.close()
