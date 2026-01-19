"""
gt - Greater than comparison (>)

When to use:
    Use gt() to compare values element-wise.
    Works with scalars or DataFrames.

Parameters:
    x: First DataFrame
    y: Second DataFrame or scalar

Example output:
    Boolean mask where price > moving average
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_mean, gt

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 20-day moving average
ma_20 = ts_mean(prices, 20)

# Compare: price > MA
above_ma = gt(prices, ma_20)

print("gt() - Greater than comparison (>)")
print("=" * 50)
print("\nPrices:")
print(prices.tail(3))
print("\n20-day MA:")
print(ma_20.tail(3))
print("\nPrice > MA (True/False):")
print(above_ma.tail(3))

# Cleanup
client.close()
