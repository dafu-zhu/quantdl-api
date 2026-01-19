"""
fundamentals - Fetch SEC filing data from S3

When to use:
    Use client.fundamentals() to fetch quarterly SEC filing data.
    Returns a wide table: rows = filing dates, columns = symbols.

Available concepts:
    rev: Revenue
    net_inc: Net Income
    ta: Total Assets
    tl: Total Liabilities
    se: Stockholders' Equity
    And more...

Parameters:
    symbols: List of ticker symbols
    concept: Fundamental concept to fetch (e.g., "rev", "net_inc")
    start: Start date
    end: End date

Example output:
    Quarterly revenue data with as_of_date as rows, symbols as columns
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient

# Initialize client
client = QuantDLClient()

# Fetch revenue data (quarterly filings)
print("Fetching revenue data...")
revenue = client.fundamentals(
    ["IBM", "JNJ"],
    concept="rev",
    start="2022-01-01",
    end="2024-12-31"
)

print(f"\nRevenue shape: {revenue.shape}")
print("\nRevenue data (quarterly, non-null rows):")
print(revenue.drop_nulls())

# Fetch net income
print("\n" + "="*50)
print("Fetching net income data...")
net_income = client.fundamentals(
    ["IBM", "JNJ"],
    concept="net_inc",
    start="2022-01-01",
    end="2024-12-31"
)

print(f"\nNet Income shape: {net_income.shape}")
print("\nNet Income data:")
print(net_income.drop_nulls())

# Cleanup
client.close()
print("\nDone!")
