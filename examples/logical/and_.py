"""
and_ - Logical AND

When to use:
    Use and_() to combine boolean conditions.
    Both conditions must be True for result to be True.

Parameters:
    x: First boolean DataFrame
    y: Second boolean DataFrame

Example output:
    Buy signal: price above MA AND positive momentum
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_mean, ts_delta, gt, and_

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create two conditions
ma_20 = ts_mean(prices, 20)
above_ma = gt(prices, ma_20)

momentum = ts_delta(prices, 5)
pos_momentum = gt(momentum, 0)

# Combine with AND
buy_signal = and_(above_ma, pos_momentum)

print("and_() - Logical AND")
print("=" * 50)
print("\nAbove 20-day MA:")
print(above_ma.tail(3))
print("\nPositive 5-day momentum:")
print(pos_momentum.tail(3))
print("\nBuy signal (above MA AND positive momentum):")
print(buy_signal.tail(3))

# Cleanup
client.close()
