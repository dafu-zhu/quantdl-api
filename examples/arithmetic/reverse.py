"""
reverse - Negation (-x)

When to use:
    Use reverse() to negate values.
    Useful for converting momentum to mean-reversion signals.

Parameters:
    x: Input DataFrame

Example output:
    Negative momentum (for mean reversion)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import reverse, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate momentum
momentum = ts_delta(prices, 5)

# Reverse momentum for mean reversion
neg_momentum = reverse(momentum)

print("reverse() - Negation (-x)")
print("=" * 50)
print("\nOriginal momentum:")
print(momentum.head(3))
print("\nReversed momentum (for mean reversion):")
print(neg_momentum.head(3))

# Cleanup
client.close()
