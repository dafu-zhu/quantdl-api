"""
hump - Limit change magnitude between periods

When to use:
    Use hump() to smooth signals by limiting step changes.
    Prevents whipsaws and sudden jumps in trading signals.

Parameters:
    x: Input DataFrame
    hump: Maximum allowed change per period

Example output:
    Smoothed z-score with limited step changes
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import hump, ts_zscore

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate price z-score
price_zscore = ts_zscore(prices, 5)

# Smooth with hump (max 0.5 change per period)
smooth_signal = hump(price_zscore, 0.5)

print("hump() - Limit change magnitude")
print("=" * 50)
print("\nOriginal z-score:")
print(price_zscore.head(7))
print("\nSmoothed z-score (max 0.5 change per period):")
print(smooth_signal.head(7))

# Cleanup
client.close()
