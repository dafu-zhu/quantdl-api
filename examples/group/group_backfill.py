"""
group_backfill - Fill NaN with winsorized group mean

When to use:
    Use group_backfill() to fill missing data with sector averages.
    Applies winsorization before computing group mean.

Parameters:
    x: Input DataFrame (signal with potential NaN)
    groups: DataFrame with group IDs (same shape as x)
    d: Lookback window for computing group mean
    std: Winsorization threshold (default 4.0)

Example output:
    Momentum with NaN filled using sector mean
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "META", "BMY", "JNJ", "LMT", "GD", "SO", "NEE"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Define sector groups
sector_map = {
    "IBM": 1, "TXN": 1, "NOW": 1, "META": 1,
    "BMY": 2, "JNJ": 2,
    "LMT": 3, "GD": 3,
    "SO": 4, "NEE": 4,
}

# Create groups DataFrame
date_col = prices.columns[0]
value_cols = prices.columns[1:]
groups = prices.select(
    pl.col(date_col),
    *[pl.lit(sector_map.get(c, 0)).alias(c) for c in value_cols]
)

# Calculate momentum (will have NaN in early rows)
momentum = ts_delta(prices, 5)

print("group_backfill() - Fill NaN with group mean")
print("=" * 50)
print("\ngroup_backfill fills NaN values with winsorized group mean")
print("Parameters:")
print("  d: lookback window for computing group mean")
print("  std: winsorization threshold (default 4.0)")
print("\nExample usage:")
print("  filled = group_backfill(momentum, groups, d=10, std=4.0)")

# Cleanup
client.close()
