"""
not_ - Logical NOT

When to use:
    Use not_() to invert boolean values.
    True becomes False, False becomes True.

Parameters:
    x: Boolean DataFrame

Example output:
    Not volatile (calm days)
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import divide, gt, lt, not_, or_, ts_delay, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily returns
daily_change = ts_delta(prices, 1)
lagged = ts_delay(prices, 1)
daily_return = divide(daily_change, lagged)

# Find volatile days
big_up = gt(daily_return, 0.02)
big_down = lt(daily_return, -0.02)
volatile = or_(big_up, big_down)

# Invert to get calm days
not_volatile = not_(volatile)

print("not_() - Logical NOT")
print("=" * 50)
print("\nVolatile days:")
print(volatile.head(3))
print("\nNot volatile (calm days):")
print(not_volatile.head(3))

# Cleanup
client.close()
