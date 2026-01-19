"""
power - Element-wise exponentiation (x^y)

When to use:
    Use power() to raise values to a power.
    Useful for non-linear transformations like squaring.

Parameters:
    x: Base DataFrame
    y: Exponent DataFrame (must have same shape as x)

Example output:
    Prices squared
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import power

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create exponent DataFrame (constant 2.0 for squaring)
date_col = prices.columns[0]
value_cols = prices.columns[1:]
exponent = prices.select(pl.col(date_col), *[pl.lit(2.0).alias(c) for c in value_cols])

# Compute prices squared
squared = power(prices, exponent)

print("power() - Element-wise exponentiation (x^y)")
print("=" * 50)
print("\nOriginal prices:")
print(prices.head())
print("\nPrices squared:")
print(squared.head())

# Cleanup
client.close()
