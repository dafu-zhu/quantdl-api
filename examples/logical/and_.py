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
from quantdl.operators import and_, gt, ts_delta, ts_mean

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create two conditions
ma_5 = ts_mean(prices, 5)
above_ma = gt(prices, ma_5)

momentum = ts_delta(prices, 3)
pos_momentum = gt(momentum, 0)

# Combine with AND
buy_signal = and_(above_ma, pos_momentum)

print("and_() - Logical AND")
print("=" * 50)
print("\nAbove 5-day MA:")
print(above_ma.head(7))
print("\nPositive 3-day momentum:")
print(pos_momentum.head(7))
print("\nBuy signal (above MA AND positive momentum):")
print(buy_signal.head(7))

# Cleanup
client.close()
