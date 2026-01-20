"""
power - Element-wise exponentiation (x^y)

When to use:
    Use power() to raise values to a power.
    Useful for non-linear transformations like squaring.

Parameters:
    x: Base DataFrame
    y: Exponent (scalar or DataFrame)

Example output:
    Prices squared
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import power

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Compute prices squared (y can be scalar)
squared = power(prices, 2)

# Compute 0.5^prices (x can be scalar)
half_power = power(0.5, prices)

print("power() - Element-wise exponentiation (x^y)")
print("=" * 50)
print("\nOriginal prices:")
print(prices.head())
print("\nPrices squared - power(prices, 2):")
print(squared.head())
print("\n0.5^prices - power(0.5, prices):")
print(half_power.head())

# Cleanup
client.close()
