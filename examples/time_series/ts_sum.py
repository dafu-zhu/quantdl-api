"""
ts_sum - Rolling sum

When to use:
    Use ts_sum() to compute cumulative values over a window.
    Useful for rolling volume, cumulative returns, etc.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    20-day cumulative volume
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_sum

# Initialize client
client = QuantDLClient()

# Fetch volume data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
volume = client.ticks(symbols, field="volume", start="2024-01-01", end="2024-06-30")

# Calculate 20-day cumulative volume
vol_20d = ts_sum(volume, 20)

print("ts_sum() - Rolling sum")
print("=" * 50)
print("\nDaily volume:")
print(volume.tail(5))
print("\n20-day cumulative volume:")
print(vol_20d.tail(5))

# Cleanup
client.close()
