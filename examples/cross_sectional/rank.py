"""
rank - Cross-sectional rank [0, 1]

When to use:
    Use rank() to rank stocks at each date.
    Returns 0 for lowest, 1 for highest in the universe.

Parameters:
    x: Input DataFrame
    rate: Controls ranking precision (default: 2).
        rate=0: Precise ordinal ranking
        rate>0: Bucket-based approximate ranking (for large universes)
        For small datasets (<32 symbols), always uses precise ranking.

Example output:
    Cross-sectional rank of prices
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import rank

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Cross-sectional rank
price_rank = rank(prices)

print("rank() - Cross-sectional rank [0, 1]")
print("=" * 50)
print("\nPrices:")
print(prices.head(3))
print("\nCross-sectional rank (0 = lowest, 1 = highest):")
print(price_rank.head(3))

# Precise ranking with rate=0
rank_precise = rank(prices, rate=0)
print("\nPrecise rank (rate=0):")
print(rank_precise.head(3))

# Cleanup
client.close()
