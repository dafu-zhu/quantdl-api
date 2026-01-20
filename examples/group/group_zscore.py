"""
group_zscore - Z-score within groups

When to use:
    Use group_zscore() to standardize within sector/group.
    Returns z-scores with mean=0, std=1 within each group.

Parameters:
    x: Input DataFrame (signal)
    groups: DataFrame with group IDs (same shape as x)

Example output:
    Momentum z-score within sector
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import group_zscore, ts_delta

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

# Z-score within sector
sector_zscore = group_zscore(momentum, groups)

print("group_zscore() - Z-score within groups")
print("=" * 50)
print("\n5-day momentum:")
print(momentum.head(7))
print("\nMomentum z-score within sector:")
print(sector_zscore.head(7))

# Cleanup
client.close()
