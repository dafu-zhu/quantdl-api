"""
trade_when - Stateful entry/exit trading logic

When to use:
    Use trade_when() for strategy implementation with entry/exit triggers.
    Maintains position state: enters on entry trigger, exits on exit trigger.

Parameters:
    entry: Entry trigger (> 0 means enter)
    alpha: Alpha signal to use when in position
    exit_: Exit trigger (> 0 means exit)

Example output:
    Trading signal (NaN when not in position)
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import zscore, gt, lt, if_else, trade_when

# Initialize client
client = QuantDLClient()

# Fetch price data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")

# Create z-score signal
cs_zscore = zscore(prices)

# Define entry and exit triggers
# Entry when z-score > 1, exit when z-score < 0
entry_trigger = gt(cs_zscore, 1.0)  # Boolean
exit_trigger = lt(cs_zscore, 0.0)   # Boolean

# Convert boolean to numeric (trade_when expects > 0 as True)
entry_numeric = if_else(entry_trigger, 1.0, 0.0)
exit_numeric = if_else(exit_trigger, 1.0, 0.0)

# Alpha signal to use when in position
alpha_signal = cs_zscore

# Create stateful trading signal
trade_signal = trade_when(entry_numeric, alpha_signal, exit_numeric)

print("trade_when() - Stateful entry/exit logic")
print("=" * 50)
print("\nEntry trigger (z-score > 1):")
print(entry_trigger.tail(5))
print("\nExit trigger (z-score < 0):")
print(exit_trigger.tail(5))
print("\nTrade signal (NaN = no position):")
print(trade_signal.tail(10))

# Cleanup
client.close()
