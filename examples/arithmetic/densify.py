"""
densify - Remap unique values to consecutive integers 0..n-1

When to use:
    Use densify() to convert categorical or bucketed data to dense indices.
    Useful after bucket() to ensure consecutive indices for downstream processing.

Parameters:
    x: Input DataFrame

Example output:
    Densified bucket indices (consecutive 0, 1, 2, ...)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import bucket, densify, rank

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create ranked data and bucket it
ranked = rank(prices)
bucketed = bucket(ranked, range_spec="0,1,0.2")  # 5 buckets

# Densify to ensure consecutive indices
dense = densify(bucketed)

print("densify() - Remap to consecutive integers")
print("=" * 50)
print("\nBucketed data (may have gaps):")
print(bucketed.head())
print("\nDensified indices (consecutive 0..n-1):")
print(dense.head())

# Cleanup
client.close()
