"""
ts_std - Rolling standard deviation

When to use:
    Use ts_std() to measure volatility over a window.
    Essential for risk metrics and volatility-adjusted signals.

Parameters:
    x: Input DataFrame
    d: Window size (number of periods)

Example output:
    20-day rolling volatility of returns
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import divide, ts_delay, ts_delta, ts_std

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily returns
daily_change = ts_delta(prices, 1)
lagged = ts_delay(prices, 1)
daily_return = divide(daily_change, lagged)

# Calculate 20-day rolling volatility
volatility = ts_std(daily_return, 20)

print("ts_std() - Rolling standard deviation")
print("=" * 50)
print("\nDaily returns:")
print(daily_return.tail(5))
print("\n20-day rolling volatility:")
print(volatility.tail(5))

# Cleanup
client.close()
