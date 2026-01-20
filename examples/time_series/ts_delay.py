"""
ts_delay - Lag values by d days

When to use:
    Use ts_delay() to access past values.
    Essential for computing returns, lead/lag analysis, etc.

Parameters:
    x: Input DataFrame
    d: Number of periods to lag

Example output:
    Prices from 5 days ago
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_delay

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Get prices from 5 days ago
prices_5d_ago = ts_delay(prices, 5)

print("ts_delay() - Lag values by d days")
print("=" * 50)
print("\nCurrent prices:")
print(prices.head(7))
print("\nPrices 5 days ago:")
print(prices_5d_ago.head(7))

# Cleanup
client.close()
