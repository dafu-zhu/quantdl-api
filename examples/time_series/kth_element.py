"""
kth_element - Get k-th element from lookback window

When to use:
    Use kth_element() to access a specific past value.
    Useful for comparing current value to a specific historical point.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)
    k: Which element to retrieve (1 = oldest, d = newest)

Example output:
    3rd element in 5-day lookback window
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import kth_element

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Get 3rd element in 5-day window
third_from_last = kth_element(prices, 5, 3)

print("kth_element() - Get k-th element in window")
print("=" * 50)
print("\nPrices:")
print(prices.head(5))
print("\n3rd element in 5-day window:")
print(third_from_last.head(5))

# Cleanup
client.close()
