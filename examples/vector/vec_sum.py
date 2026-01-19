"""
vec_sum - Sum of list elements

When to use:
    Use vec_sum() when cells contain lists (e.g., multiple values to aggregate).
    Computes sum of each list.

Parameters:
    x: DataFrame with list-type columns

Example output:
    Sum of analyst price targets
"""
from dotenv import load_dotenv

load_dotenv()

from datetime import date

import polars as pl

from quantdl.operators import vec_sum

# Create sample data with list-type columns
# Example: multiple analyst price targets per stock
list_data = pl.DataFrame({
    "timestamp": [date(2024, 1, 1), date(2024, 1, 2)],
    "IBM": [[180.0, 185.0, 190.0], [182.0, 187.0]],
    "TXN": [[200.0, 205.0], [210.0, 215.0, 220.0]],
})

print("vec_sum() - Sum of list elements")
print("=" * 50)
print("\nData with list columns:")
print(list_data)

# Compute sum of list elements
sum_targets = vec_sum(list_data)
print("\nSum of list elements:")
print(sum_targets)
