"""
ge - Greater than or equal comparison (>=)

When to use:
    Use ge() to compare values element-wise.
    Works with scalars or DataFrames.

Parameters:
    x: First DataFrame
    y: Second DataFrame or scalar

Example output:
    Boolean mask where price >= moving average
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ge, ts_mean

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 20-day moving average
ma_20 = ts_mean(prices, 20)

# Compare: price >= MA
at_or_above = ge(prices, ma_20)

print("ge() - Greater than or equal comparison (>=)")
print("=" * 50)
print("\nPrices:")
print(prices.tail(3))
print("\n20-day MA:")
print(ma_20.tail(3))
print("\nPrice >= MA (True/False):")
print(at_or_above.tail(3))

# Cleanup
client.close()
