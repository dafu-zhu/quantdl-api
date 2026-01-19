"""
min - Element-wise minimum across DataFrames

When to use:
    Use min() to get the lowest value at each position across DataFrames.
    Useful for lower envelopes, support levels, etc.

Parameters:
    *args: Two or more DataFrames to compare

Example output:
    Lower envelope (min of 5d and 20d moving average)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import min as ops_min
from quantdl.operators import ts_mean

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate moving averages
ma_5 = ts_mean(prices, 5)
ma_20 = ts_mean(prices, 20)

# Compute lower envelope
ma_lower = ops_min(ma_5, ma_20)

print("min() - Element-wise minimum across DataFrames")
print("=" * 50)
print("\n5-day moving average:")
print(ma_5.tail(3))
print("\n20-day moving average:")
print(ma_20.tail(3))
print("\nLower envelope (min of MA5, MA20):")
print(ma_lower.tail(3))

# Cleanup
client.close()
