"""
ts_step - Row counter (time index)

When to use:
    Use ts_step() to create a row index starting from 0.
    Useful for time-based weighting or debugging.

Parameters:
    x: Input DataFrame (uses shape, ignores values)

Example output:
    Row counter 0, 1, 2, ...
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_step

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create row counter
time_idx = ts_step(prices)

print("ts_step() - Row counter")
print("=" * 50)
print("\nRow counter (0, 1, 2, ...):")
print(time_idx.head(7))

# Cleanup
client.close()
