"""
subtract - Element-wise subtraction

When to use:
    Use subtract() to compute differences between two DataFrames.
    Useful for spread calculations, momentum differences, etc.

Parameters:
    x: First DataFrame (minuend)
    y: Second DataFrame (subtrahend)

Example output:
    Difference between short and long momentum
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import subtract, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate different momentum periods
momentum_3d = ts_delta(prices, 3)
momentum_5d = ts_delta(prices, 5)

# Compute momentum spread
momentum_spread = subtract(momentum_3d, momentum_5d)

print("subtract() - Element-wise subtraction")
print("=" * 50)
print("\n3-day momentum:")
print(momentum_3d.head(7))
print("\n5-day momentum:")
print(momentum_5d.head(7))
print("\nMomentum spread (3d - 5d):")
print(momentum_spread.head(7))

# Cleanup
client.close()
