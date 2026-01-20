"""
group_mean - Weighted mean within groups

When to use:
    Use group_mean() to compute weighted average within sector/group.
    Weights can be market cap, volume, etc.

Parameters:
    x: Input DataFrame (signal)
    weights: DataFrame with weights (same shape as x)
    groups: DataFrame with group IDs (same shape as x)

Example output:
    Market-cap weighted sector average momentum
"""
from dotenv import load_dotenv

load_dotenv()

import polars as pl

from quantdl import QuantDLClient
from quantdl.operators import group_mean, multiply, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price and volume data
symbols = ["IBM", "TXN", "NOW", "META", "BMY", "JNJ", "LMT", "GD", "SO", "NEE"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")
volume = client.ticks(symbols, field="volume", start="2024-01-01", end="2024-06-30")

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

# Calculate momentum and market cap proxy (price * volume)
momentum = ts_delta(prices, 5)
market_cap_proxy = multiply(prices, volume)

# Weighted sector mean
sector_avg = group_mean(momentum, market_cap_proxy, groups)

print("group_mean() - Weighted mean within groups")
print("=" * 50)
print("\n5-day momentum:")
print(momentum.head(7))
print("\nMarket-cap weighted sector average momentum:")
print(sector_avg.head(7))

# Cleanup
client.close()
