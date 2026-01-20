"""
ts_av_diff - Deviation from rolling mean

When to use:
    Use ts_av_diff() to measure how far current value is from average.
    Similar to mean reversion signal (high = overbought, low = oversold).

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    Deviation from 5-day mean
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_av_diff

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate deviation from 5-day mean
price_dev = ts_av_diff(prices, 5)

print("ts_av_diff() - Deviation from rolling mean")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\nDeviation from 5-day mean:")
print(price_dev.head(7))

# Cleanup
client.close()
