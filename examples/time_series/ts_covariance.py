"""
ts_covariance - Rolling covariance between two DataFrames

When to use:
    Use ts_covariance() to measure how two series move together.
    Unlike correlation, covariance is not normalized.

Parameters:
    x: First DataFrame
    y: Second DataFrame
    d: Window size (number of periods)

Example output:
    5-day rolling price-volume covariance
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_covariance

# Initialize client
client = QuantDLClient()

# Fetch price and volume data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")
volume = client.ticks(symbols, field="volume", start="2024-01-01", end="2024-06-30")

# Calculate rolling covariance
price_vol_cov = ts_covariance(prices, volume, 5)

print("ts_covariance() - Rolling covariance")
print("=" * 50)
print("\n5-day rolling price-volume covariance:")
print(price_vol_cov.head(7))

# Cleanup
client.close()
