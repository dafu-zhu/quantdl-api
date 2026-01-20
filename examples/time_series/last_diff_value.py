"""
last_diff_value - Last value that was different from current

When to use:
    Use last_diff_value() to find when a signal changed.
    Useful for detecting regime changes or level transitions.

Parameters:
    x: Input DataFrame
    d: Maximum lookback window (cannot equals 1)

Example output:
    Last different value in discretized signal
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import bucket, last_diff_value, rank

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create discrete signal (buckets)
discrete_signal = bucket(rank(prices), range_spec="0,1,0.25")

# Find last different value
last_different = last_diff_value(discrete_signal, 3)

print("last_diff_value() - Last different value")
print("=" * 50)
print("\nDiscrete signal (buckets):")
print(discrete_signal.head(7))
print("\nLast different value:")
print(last_different.head(7))

# Cleanup
client.close()
