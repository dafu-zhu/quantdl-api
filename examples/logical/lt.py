"""
lt - Less than comparison (<)

When to use:
    Use lt() to compare values element-wise.
    Works with scalars or DataFrames.

Parameters:
    x: First DataFrame
    y: Second DataFrame or scalar

Example output:
    Boolean mask where price < moving average
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import lt, ts_mean

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 5-day moving average
ma_5 = ts_mean(prices, 5)

# Compare: price < MA
below_ma = lt(prices, ma_5)

print("lt() - Less than comparison (<)")
print("=" * 50)
print("\nPrices:")
print(prices.head(7))
print("\n5-day MA:")
print(ma_5.head(7))
print("\nPrice < MA (True/False):")
print(below_ma.head(7))

# Cleanup
client.close()
