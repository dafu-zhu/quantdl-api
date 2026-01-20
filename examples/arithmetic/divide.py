"""
divide - Safe element-wise division

When to use:
    Use divide() for division that handles div-by-zero gracefully.
    Returns null for divisions by zero instead of raising an error.

Parameters:
    x: Numerator DataFrame
    y: Denominator DataFrame

Example output:
    Daily returns calculated as (P_t - P_{t-1}) / P_{t-1}
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import divide, ts_delay, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily change and lagged prices
daily_change = ts_delta(prices, 1)
lagged_prices = ts_delay(prices, 1)

# Calculate daily returns using safe division
daily_return = divide(daily_change, lagged_prices)

print("divide() - Safe element-wise division")
print("=" * 50)
print("\nDaily price change:")
print(daily_change.head(3))
print("\nLagged prices:")
print(lagged_prices.head(3))
print("\nDaily returns (safe division - null if denominator is 0):")
print(daily_return.head(3))

# Cleanup
client.close()
