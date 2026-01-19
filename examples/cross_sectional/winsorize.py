"""
winsorize - Clip outliers to mean +/- n*std

When to use:
    Use winsorize() to limit extreme values.
    Clips values outside mean +/- n*std to that boundary.

Parameters:
    x: Input DataFrame
    std: Number of standard deviations for clipping threshold

Example output:
    Winsorized momentum (clipped to +/-2 std)
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_delta, winsorize

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate momentum
momentum = ts_delta(prices, 20)

# Winsorize to +/- 2 std
winsorized = winsorize(momentum, std=2.0)

print("winsorize() - Clip to mean +/- n*std")
print("=" * 50)
print("\nOriginal 20-day momentum:")
print(momentum.tail(3))
print("\nWinsorized (+/-2 std):")
print(winsorized.tail(3))

# Cleanup
client.close()
