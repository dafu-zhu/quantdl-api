"""
multiply - Element-wise multiplication (variadic)

When to use:
    Use multiply() to combine factors or weight signals.
    Accepts any number of DataFrames - useful for volume-weighted signals.

Parameters:
    *args: Two or more DataFrames to multiply together

Example output:
    Volume-weighted price change
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import multiply, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price and volume data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")
volume = client.ticks(symbols, field="volume", start="2024-01-01", end="2024-06-30")

# Calculate daily price change
daily_change = ts_delta(prices, 1)

# Volume-weighted price change
weighted = multiply(daily_change, volume)

print("multiply() - Element-wise multiplication (variadic)")
print("=" * 50)
print("\nDaily price change:")
print(daily_change.head(3))
print("\nVolume:")
print(volume.head(3))
print("\nVolume-weighted price change:")
print(weighted.head(3))

# Cleanup
client.close()
