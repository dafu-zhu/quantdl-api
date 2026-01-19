"""
inverse - Compute 1/x with null handling

When to use:
    Use inverse() to compute reciprocals safely.
    Returns null for zero values instead of raising an error.

Parameters:
    x: Input DataFrame

Example output:
    Inverse of prices (1/price)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import inverse

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Compute inverse of prices
inv_prices = inverse(prices)

print("inverse() - Compute 1/x with null handling")
print("=" * 50)
print("\nOriginal prices:")
print(prices.head())
print("\nInverse of prices (1/price):")
print(inv_prices.head())

# Cleanup
client.close()
