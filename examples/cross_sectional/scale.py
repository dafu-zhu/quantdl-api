"""
scale - Scale to target absolute sum (for portfolio weights)

When to use:
    Use scale() to convert signals to portfolio weights.
    longscale/shortscale allow asymmetric long/short scaling.

Parameters:
    x: Input DataFrame
    scale: Target absolute sum (default 1.0)
    longscale: Scale for positive values (optional)
    shortscale: Scale for negative values (optional)

Example output:
    Portfolio weights with |sum| = 1
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import normalize, scale, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate demeaned momentum
momentum = ts_delta(prices, 5)
demeaned = normalize(momentum)

# Scale to portfolio weights
weights = scale(demeaned, scale=1.0)

print("scale() - Scale to target absolute sum")
print("=" * 50)
print("\nDemeaned momentum:")
print(demeaned.head(7))
print("\nPortfolio weights (|sum| = 1):")
print(weights.head(7))

# Verify absolute sum
date_col = weights.columns[0]
value_cols = weights.columns[1:]
abs_sums = weights.select(pl.sum_horizontal(*[pl.col(c).abs() for c in value_cols])).to_series()
print(f"\nAbs sums (should be ~1): {abs_sums.head(7).to_list()}")

# Asymmetric scaling (60% long, 40% short)
weights_asym = scale(demeaned, longscale=0.6, shortscale=0.4)
print("\nAsymmetric weights (60% long, 40% short):")
print(weights_asym.head(7))

# Cleanup
client.close()
