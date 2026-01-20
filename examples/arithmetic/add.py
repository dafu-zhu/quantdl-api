"""
add - Element-wise addition (variadic)

When to use:
    Use add() to combine multiple signals or DataFrames element-wise.
    Accepts any number of DataFrames - useful for multi-factor alpha combination.

Parameters:
    *args: Two or more DataFrames to add together

Example output:
    Combined signal from multiple indicators
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import add, ts_zscore

# Initialize client
client = QuantDLClient()

# Fetch price and volume data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")
volume = client.ticks(symbols, field="volume", start="2024-01-01", end="2024-06-30")

# Create two signals
price_signal = ts_zscore(prices, 5)
vol_signal = ts_zscore(volume, 5)

# Combine signals with add (variadic - can add more DataFrames)
combined = add(price_signal, vol_signal)

print("add() - Element-wise addition (variadic)")
print("=" * 50)
print("\nPrice z-score signal:")
print(price_signal.head(7))
print("\nVolume z-score signal:")
print(vol_signal.head(7))
print("\nCombined signal (price + volume z-scores):")
print(combined.head(7))

# Example with 3 inputs (variadic)
triple_combined = add(price_signal, vol_signal, price_signal)
print("\nTriple combined (price + vol + price):")
print(triple_combined.head(7))

# Cleanup
client.close()
