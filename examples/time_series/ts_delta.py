"""
ts_delta - Difference from d days ago

When to use:
    Use ts_delta() to calculate momentum or price changes.
    Computes x[t] - x[t-d].

Parameters:
    x: Input DataFrame
    d: Lookback period

Example output:
    20-day price change (momentum)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data (use longer range to have valid data after lookback)
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate 20-day momentum
momentum_20d = ts_delta(prices, 20)

print("ts_delta() - Difference from d days ago")
print("=" * 50)
print("\nPrices (first 25 rows):")
print(prices.head(21))
print("\n20-day price change (first 25 rows):")
print(momentum_20d.head(21))
print("\nNote: First 20 rows are null (need 20 days of history)")

# Cleanup
client.close()
