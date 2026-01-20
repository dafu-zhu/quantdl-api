"""
metrics - Fetch derived financial metrics from S3

When to use:
    Use client.metrics() to fetch pre-computed financial ratios.
    Returns a wide table: rows = dates, columns = symbols.

Available metrics:
    pe_ratio: Price to Earnings ratio
    pb_ratio: Price to Book ratio
    roe: Return on Equity
    And more...

Parameters:
    symbols: List of ticker symbols
    metric: Metric name to fetch
    start: Start date
    end: End date

Note:
    Metrics are derived from fundamentals and may not be available
    for all symbols or time periods.
"""
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient

# Initialize client
client = QuantDLClient()

# Fetch Gross Profit metric
print("Fetching Gross Profit...")
try:
    grs_pft = client.metrics(
        ["IBM", "JNJ"],
        metric="grs_pft",
        start="2022-01-01",
        end="2024-12-31"
    )
    print(f"\nGross Profit shape: {grs_pft.shape}")
    print("\nGross Profit data:")
    print(grs_pft.drop_nulls())
except Exception as e:
    print(f"\nMetrics not available: {e}")
    print("This is expected if derived metrics haven't been computed yet.")

# Cleanup
client.close()
print("\nDone!")
