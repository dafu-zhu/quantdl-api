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
    concept: Fundamental concept to fetch (e.g., "rev", "net_inc", "ta")
    start: Start date
    end: End date
    source: "raw" for quarterly filings, "ttm" for trailing twelve months
            Defaults to TTM for duration concepts, raw for balance sheet items.

Duration concepts (default TTM): rev, cor, op_inc, net_inc, ibt, inc_tax_exp,
    int_exp, rnd, sga, dna, cfo, cfi, cff, capex, div, sto_isu

Balance sheet concepts (default raw): ta, tl, se, cash, debt, etc.

Example output:
    Raw: IBM Q1 2022 revenue = $14.2B (single quarter)
    TTM: IBM Q1 2022 revenue = $57.4B (sum of last 4 quarters)
"""
import polars as pl
from dotenv import load_dotenv

load_dotenv()

from quantdl import QuantDLClient

# Initialize client
client = QuantDLClient()

# Fetch TTM revenue (default for duration concepts like rev)
print("=" * 70)
print("TTM REVENUE (default for 'rev') - Sum of last 4 quarters")
print("=" * 70)
revenue_ttm = client.fundamentals(
    ["IBM"],
    concept="rev",
    start="2022-01-01",
    end="2024-12-31",
    # source defaults to "ttm" for duration concepts
)

# Show all rows with data
symbol_cols = [c for c in revenue_ttm.columns if c != "timestamp"]
has_data = pl.any_horizontal([pl.col(c).is_not_null() for c in symbol_cols])
ttm_data = revenue_ttm.filter(has_data)
print(f"\nFound {len(ttm_data)} TTM data points:\n")
print(ttm_data)

# Fetch quarterly revenue (explicit raw)
print("\n" + "=" * 70)
print("QUARTERLY REVENUE (source='raw') - Single quarter values")
print("=" * 70)
revenue = client.fundamentals(
    ["IBM"],
    concept="rev",
    start="2022-01-01",
    end="2024-12-31",
    source="raw",  # explicit override for quarterly values
)

symbol_cols = [c for c in revenue.columns if c != "timestamp"]
has_data = pl.any_horizontal([pl.col(c).is_not_null() for c in symbol_cols])
quarterly_data = revenue.filter(has_data)
print(f"\nFound {len(quarterly_data)} quarterly filings:\n")
print(quarterly_data)

# Compare quarterly vs TTM for same dates
print("\n" + "=" * 70)
print("COMPARISON: Quarterly vs TTM Revenue")
print("=" * 70)
print("\nQuarterly = single quarter revenue")
print("TTM = trailing 12-month revenue (sum of 4 quarters)")
print("\nExample: IBM Q1 2022")
print("  Quarterly: ~$14-17B (one quarter)")
print("  TTM:       ~$57-60B (four quarters summed)")

# Cleanup
client.close()
