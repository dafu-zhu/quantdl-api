"""
signed_power - Sign-preserving exponentiation: sign(x) * |x|^y

When to use:
    Use signed_power() for non-linear transformations that preserve direction.
    Unlike power(), this keeps negative values negative after transformation.

Parameters:
    x: Base DataFrame
    y: Exponent DataFrame (must have same shape as x)

Example output:
    Signed sqrt of returns (preserves direction)
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import log, signed_power, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate log returns
log_prices = log(prices)
returns = ts_delta(log_prices, 1)

# Create exponent DataFrame (0.5 for sqrt)
date_col = prices.columns[0]
value_cols = prices.columns[1:]
exp_half = prices.select(pl.col(date_col), *[pl.lit(0.5).alias(c) for c in value_cols])

# Compute signed sqrt of returns
sqrt_returns = signed_power(returns, exp_half)

print("signed_power() - Sign-preserving exponentiation")
print("=" * 50)
print("\nOriginal returns (can be negative):")
print(returns.head(5))
print("\nSigned sqrt of returns (preserves direction):")
print(sqrt_returns.head(5))

# Cleanup
client.close()
