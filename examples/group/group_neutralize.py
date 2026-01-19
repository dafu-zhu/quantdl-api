"""
group_neutralize - Subtract group mean (sector-neutral)

When to use:
    Use group_neutralize() to create sector-neutral signals.
    Subtracts the sector/group average from each stock.

Parameters:
    x: Input DataFrame (signal)
    groups: DataFrame with group IDs (same shape as x)

Example output:
    Sector-neutral momentum
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import group_neutralize, ts_delta

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
momentum = ts_delta(prices, 20)

# Sector-neutralize
sector_neutral = group_neutralize(momentum, groups)

print("group_neutralize() - Sector-neutral signal")
print("=" * 50)
print("\n20-day momentum:")
print(momentum.tail(3))
print("\nSector-neutral momentum (subtract sector mean):")
print(sector_neutral.tail(3))

# Cleanup
client.close()
