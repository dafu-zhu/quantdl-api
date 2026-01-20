"""
days_from_last_change - Days since value changed

When to use:
    Use days_from_last_change() to measure regime duration.
    Counts days since the value was different.

Parameters:
    x: Input DataFrame

Example output:
    Days since signal bucket changed
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import bucket, days_from_last_change, rank

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create discrete signal (buckets)
discrete_signal = bucket(rank(prices), range_spec="0,1,0.25")

# Find days since change
days_unchanged = days_from_last_change(discrete_signal)

print("days_from_last_change() - Days since value changed")
print("=" * 50)
print("\nDiscrete signal:")
print(discrete_signal.head(7))
print("\nDays since signal changed:")
print(days_unchanged.head(7))

# Cleanup
client.close()
