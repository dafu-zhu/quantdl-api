"""
bucket - Discretize to bucket indices

When to use:
    Use bucket() to convert continuous signals to discrete buckets.
    Useful for quintile portfolios, signal discretization, etc.

Parameters:
    x: Input DataFrame
    range_spec: "start,end,step" for evenly spaced boundaries
    buckets: Explicit boundary list as string "b1,b2,b3"
    skipBegin/skipEnd/skipBoth: Exclude edge buckets
    NANGroup: Assign NaN to separate bucket

Example output:
    Z-score discretized into buckets
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_delta, ts_delay, divide, zscore, bucket

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily returns and z-score
daily_change = ts_delta(prices, 1)
lagged = ts_delay(prices, 1)
daily_return = divide(daily_change, lagged)
cs_zscore = zscore(daily_return)

print("bucket() - Discretize to buckets")
print("=" * 50)

# Example 1: range_spec for evenly spaced boundaries
momentum_buckets = bucket(cs_zscore, range_spec="-2,2,0.5")
print("\nZ-score buckets (range_spec='-2,2,0.5'):")
print("Boundaries at -2, -1.5, -1, ..., 1.5, 2")
print(momentum_buckets.tail(3))

# Example 2: Explicit boundaries
momentum_quintiles = bucket(cs_zscore, buckets="-1.5,-0.5,0.5,1.5")
print("\nQuintiles (buckets='-1.5,-0.5,0.5,1.5'):")
print(momentum_quintiles.tail(3))

# Example 3: Skip edge buckets
inner_buckets = bucket(cs_zscore, range_spec="-1,1,0.5", skipBoth=True)
print("\nInner buckets only (skipBoth=True):")
print(inner_buckets.tail(3))

# Example 4: NANGroup
with_nan = bucket(daily_return, range_spec="-0.02,0.02,0.01", NANGroup=True)
print("\nReturns with NANGroup (first row NaN -> special bucket):")
print(with_nan.head(3))

# Cleanup
client.close()
