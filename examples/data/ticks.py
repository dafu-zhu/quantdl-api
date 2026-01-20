"""
ticks - Fetch daily OHLCV price data from S3

When to use:
    Use client.ticks() to fetch daily price/volume data for alpha research.
    Returns a wide table: rows = dates, columns = symbols.

Parameters:
    symbols: List of ticker symbols (e.g., ["IBM", "AAPL"])
    field: One of "open", "high", "low", "close", "volume"
    start: Start date (YYYY-MM-DD string or date object)
    end: End date (YYYY-MM-DD string or date object)

Example output:
    shape: (125, 6)
    ┌────────────┬─────────┬─────────┬─────────┬─────────┬─────────┐
    │ timestamp  ┆ IBM     ┆ TXN     ┆ NOW     ┆ BMY     ┆ LMT     │
    │ ---        ┆ ---     ┆ ---     ┆ ---     ┆ ---     ┆ ---     │
    │ date       ┆ f64     ┆ f64     ┆ f64     ┆ f64     ┆ f64     │
    ╞════════════╪═════════╪═════════╪═════════╪═════════╪═════════╡
    │ 2024-01-02 ┆ 162.66  ┆ 169.92  ┆ 703.02  ┆ 51.30   ┆ 453.10  │
    └────────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient

# Initialize client - connects to S3 us-equity-datalake bucket
client = QuantDLClient()

# Define symbols with verified S3 data coverage
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]

# Fetch daily close prices
print("Fetching daily close prices...")
prices = client.ticks(
    symbols,
    field="close",
    start="2024-01-01",
    end="2024-06-30"
)

print(f"\nShape: {prices.shape}")
print("\nFirst 7 rows (raw data has no nulls):")
print(prices.head(7))

# Fetch volume data
print("\n" + "="*50)
print("Fetching volume data...")
volume = client.ticks(
    symbols,
    field="volume",
    start="2024-01-01",
    end="2024-06-30"
)

print(f"\nVolume shape: {volume.shape}")
print("\nVolume sample:")
print(volume.head())

# Cleanup
client.close()
print("\nDone!")
