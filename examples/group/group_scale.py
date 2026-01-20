"""
group_scale - Min-max scale within groups [0, 1]

When to use:
    Use group_scale() to normalize to [0, 1] within sector/group.
    Shows relative position within group.

Parameters:
    x: Input DataFrame (signal)
    groups: DataFrame with group IDs (same shape as x)

Example output:
    Momentum scaled within sector
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import group_scale, ts_delta

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

# Calculate momentum
momentum = ts_delta(prices, 5)

# Scale within sector
sector_scaled = group_scale(momentum, groups)

print("group_scale() - Min-max scale within groups")
print("=" * 50)
print("\n5-day momentum:")
print(momentum.head(7))
print("\nMomentum scaled within sector [0, 1]:")
print(sector_scaled.head(7))

# Cleanup
client.close()
