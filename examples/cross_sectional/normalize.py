"""
normalize - Demean (subtract row mean)

When to use:
    Use normalize() to center values around zero at each date.
    Row sums will be approximately 0.

Parameters:
    x: Input DataFrame

Example output:
    Demeaned momentum (row sums ~ 0)
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import normalize, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate momentum
momentum = ts_delta(prices, 5)

# Demean
demeaned = normalize(momentum)

print("normalize() - Demean (row sums ~ 0)")
print("=" * 50)
print("\n5-day momentum:")
print(momentum.head(7))
print("\nDemeaned momentum:")
print(demeaned.head(7))

# Verify row sums
date_col = demeaned.columns[0]
row_sums = demeaned.select(pl.sum_horizontal(pl.exclude(date_col))).to_series()
print(f"\nRow sums (should be ~0): {row_sums.head(7).to_list()}")

# Cleanup
client.close()
