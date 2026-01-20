"""
if_else - Conditional selection

When to use:
    Use if_else() to select values based on condition.
    Works with scalar or DataFrame branches.

Parameters:
    condition: Boolean DataFrame
    then_value: Value if True (scalar or DataFrame)
    else_value: Value if False (scalar or DataFrame)

Example output:
    Capped returns (+/- 5%) and adaptive alpha
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import divide, gt, if_else, lt, rank, reverse, ts_delay, ts_delta, ts_mean

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Calculate daily returns
daily_change = ts_delta(prices, 1)
lagged = ts_delay(prices, 1)
daily_return = divide(daily_change, lagged)

# Example 1: Cap returns at +/- 5% using scalar branches
capped_return = if_else(
    gt(daily_return, 0.05),
    0.05,  # scalar: cap at +5%
    if_else(
        lt(daily_return, -0.05),
        -0.05,  # scalar: cap at -5%
        daily_return  # DataFrame: keep original
    )
)

print("if_else() - Conditional selection")
print("=" * 50)
print("\nExample 1: Cap returns at +/- 5%")
print("Original returns:")
print(daily_return.head(7))
print("Capped returns:")
print(capped_return.head(7))

# Example 2: Adaptive alpha with DataFrame branches
ma_5 = ts_mean(prices, 5)
above_ma = gt(prices, ma_5)

momentum_alpha = rank(ts_delta(prices, 5))
mean_rev_alpha = reverse(rank(ts_delta(prices, 3)))

adaptive_alpha = if_else(above_ma, momentum_alpha, mean_rev_alpha)

print("\nExample 2: Adaptive alpha")
print("Above MA -> momentum, Below MA -> mean reversion")
print("Adaptive alpha:")
print(adaptive_alpha.head(7))

# Cleanup
client.close()
