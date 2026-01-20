"""
ts_corr - Rolling correlation between two DataFrames

When to use:
    Use ts_corr() to measure co-movement over time.
    Correlates matching columns (IBM price with IBM volume, etc.).

Parameters:
    x: First DataFrame
    y: Second DataFrame
    d: Window size (number of periods)

Example output:
    5-day rolling price-volume correlation
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_corr

# Initialize client
client = QuantDLClient()

# Fetch price and volume data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")
volume = client.ticks(symbols, field="volume", start="2024-01-01", end="2024-06-30")

# Calculate rolling correlation
price_vol_corr = ts_corr(prices, volume, 5)

print("ts_corr() - Rolling correlation")
print("=" * 50)
print("\n5-day rolling price-volume correlation:")
print(price_vol_corr.head(7))

# Cleanup
client.close()
