"""
group_rank - Rank within groups

When to use:
    Use group_rank() to rank stocks within their sector/group.
    Returns 0-1 rank within each group at each date.

Parameters:
    x: Input DataFrame (signal)
    groups: DataFrame with group IDs (same shape as x)

Example output:
    Momentum rank within sector
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import group_rank, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data - use extended symbols for groups
symbols = ["IBM", "TXN", "NOW", "META", "BMY", "JNJ", "LMT", "GD", "SO", "NEE"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Define sector groups
# Tech=1, Healthcare=2, Defense=3, Utilities=4
sector_map = {
    "IBM": 1, "TXN": 1, "NOW": 1, "META": 1,  # Tech
    "BMY": 2, "JNJ": 2,                         # Healthcare
    "LMT": 3, "GD": 3,                          # Defense
    "SO": 4, "NEE": 4,                          # Utilities
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

# Rank within sector
sector_rank = group_rank(momentum, groups)

print("group_rank() - Rank within groups")
print("=" * 50)
print("\nSector groups (1=Tech, 2=Health, 3=Defense, 4=Utilities):")
print(groups.head(1))
print("\n5-day momentum:")
print(momentum.head(7))
print("\nMomentum rank within sector:")
print(sector_rank.head(7))

# Cleanup
client.close()
