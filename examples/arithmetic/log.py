"""
log - Natural logarithm with null handling

When to use:
    Use log() to compute natural logarithm safely.
    Returns null for values <= 0 instead of raising an error.
    Essential for log-returns calculation.

Parameters:
    x: Input DataFrame

Example output:
    Log of prices (for log-returns calculation)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import log

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Compute log of prices
log_prices = log(prices)

print("log() - Natural logarithm with null handling")
print("=" * 50)
print("\nOriginal prices:")
print(prices.head())
print("\nLog prices (null for values <= 0):")
print(log_prices.head())

# Cleanup
client.close()
