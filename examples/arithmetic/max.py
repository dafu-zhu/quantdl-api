"""
max - Element-wise maximum across DataFrames

When to use:
    Use max() to get the highest value at each position across DataFrames.
    Useful for upper envelopes, resistance levels, etc.

Parameters:
    *args: Two or more DataFrames to compare

Example output:
    Upper envelope (max of 5d and 20d moving average)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import max as ops_max
from quantdl.operators import ts_mean

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate moving averages
ma_3 = ts_mean(prices, 3)
ma_5 = ts_mean(prices, 5)

# Compute upper envelope
ma_upper = ops_max(ma_3, ma_5)

print("max() - Element-wise maximum across DataFrames")
print("=" * 50)
print("\n3-day moving average:")
print(ma_3.head(7))
print("\n5-day moving average:")
print(ma_5.head(7))
print("\nUpper envelope (max of MA3, MA5):")
print(ma_upper.head(7))

# Cleanup
client.close()
