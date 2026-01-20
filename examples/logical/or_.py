"""
or_ - Logical OR

When to use:
    Use or_() to combine boolean conditions.
    Either condition True makes result True.

Parameters:
    x: First boolean DataFrame
    y: Second boolean DataFrame

Example output:
    Volatile day: big move up OR big move down
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import divide, gt, lt, or_, ts_delay, ts_delta

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily returns
daily_change = ts_delta(prices, 1)
lagged = ts_delay(prices, 1)
daily_return = divide(daily_change, lagged)

# Create two conditions
big_up = gt(daily_return, 0.02)    # > 2% return
big_down = lt(daily_return, -0.02)  # < -2% return

# Combine with OR
volatile = or_(big_up, big_down)

print("or_() - Logical OR")
print("=" * 50)
print("\nBig up day (> 2%):")
print(big_up.head(3))
print("\nBig down day (< -2%):")
print(big_down.head(3))
print("\nVolatile day (big up OR big down):")
print(volatile.head(3))

# Cleanup
client.close()
