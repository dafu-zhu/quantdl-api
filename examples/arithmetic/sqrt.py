"""
sqrt - Square root with null handling

When to use:
    Use sqrt() to compute square roots safely.
    Returns null for negative values instead of raising an error.

Parameters:
    x: Input DataFrame

Example output:
    Square root of prices
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import sqrt

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Compute sqrt of prices
sqrt_prices = sqrt(prices)

print("sqrt() - Square root with null handling")
print("=" * 50)
print("\nOriginal prices:")
print(prices.head())
print("\nSqrt of prices (null for negative values):")
print(sqrt_prices.head())

# Cleanup
client.close()
