"""
zscore - Cross-sectional standardization (mean=0, std=1)

When to use:
    Use zscore() to standardize values across the universe at each date.
    Returns z-scores with mean=0 and std=1.

Parameters:
    x: Input DataFrame

Example output:
    Cross-sectional z-score of momentum
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_delta, zscore

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate momentum
momentum = ts_delta(prices, 5)

# Cross-sectional z-score
cs_zscore = zscore(momentum)

print("zscore() - Cross-sectional standardization")
print("=" * 50)
print("\n5-day momentum:")
print(momentum.head(7))
print("\nCross-sectional z-score:")
print(cs_zscore.head(7))

# Cleanup
client.close()
