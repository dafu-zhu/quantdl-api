"""
bucket - Assign values to discrete buckets

When to use:
    Use bucket() to discretize continuous values into bins.
    Useful for creating categorical signals from ranked data.
    Often paired with densify() for consecutive indices.

Parameters:
    x: Input DataFrame
    range_spec: "start,end,step" string defining bucket boundaries
        e.g., "0,1,0.25" creates 4 buckets: [0,0.25), [0.25,0.5), [0.5,0.75), [0.75,1]

Example output:
    Bucket lower bounds for each value
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import bucket, rank

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Rank prices first (values in [0, 1])
ranked = rank(prices)

# Bucket into quartiles
quartiles = bucket(ranked, range_spec="0,1,0.25")

print("bucket() - Assign values to discrete buckets")
print("=" * 50)
print("\nRanked prices [0, 1]:")
print(ranked.head(3))
print("\nQuartile buckets (0, 0.25, 0.5, 0.75):")
print(quartiles.head(3))

# Bucket into quintiles (5 buckets)
quintiles = bucket(ranked, range_spec="0,1,0.2")
print("\nQuintile buckets (0, 0.2, 0.4, 0.6, 0.8):")
print(quintiles.head(3))

# Cleanup
client.close()
