"""
ts_product - Rolling product

When to use:
    Use ts_product() to compute cumulative products over a window.
    Essential for cumulative returns calculation.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    5-day cumulative return factor
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import add, divide, ts_delay, ts_delta, ts_product

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily returns
daily_change = ts_delta(prices, 1)
lagged = ts_delay(prices, 1)
daily_return = divide(daily_change, lagged)

# Convert to return factor (1 + return)
date_col = prices.columns[0]
value_cols = prices.columns[1:]
one_df = prices.select(pl.col(date_col), *[pl.lit(1.0).alias(c) for c in value_cols])
return_factor = add(daily_return, one_df)

# Calculate 5-day cumulative return factor
cum_return = ts_product(return_factor, 5)

print("ts_product() - Rolling product")
print("=" * 50)
print("\nDaily return factors (1 + return):")
print(return_factor.head(6))
print("\n5-day cumulative return factor:")
print(cum_return.head(6))

# Cleanup
client.close()
