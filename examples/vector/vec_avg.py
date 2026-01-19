"""
vec_avg - Mean of list elements

When to use:
    Use vec_avg() when cells contain lists (e.g., multiple analyst estimates).
    Computes average of each list.

Parameters:
    x: DataFrame with list-type columns

Example output:
    Average of analyst price targets
"""
from dotenv import load_dotenv

load_dotenv()

from datetime import date

import polars as pl

from quantdl.operators import vec_avg

# Create sample data with list-type columns
# Example: multiple analyst price targets per stock
list_data = pl.DataFrame({
    "timestamp": [date(2024, 1, 1), date(2024, 1, 2)],
    "IBM": [[180.0, 185.0, 190.0], [182.0, 187.0]],
    "TXN": [[200.0, 205.0], [210.0, 215.0, 220.0]],
})

print("vec_avg() - Mean of list elements")
print("=" * 50)
print("\nData with list columns:")
print(list_data)

# Compute average of list elements
avg_targets = vec_avg(list_data)
print("\nAverage of list elements:")
print(avg_targets)
