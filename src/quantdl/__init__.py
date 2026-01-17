"""QuantDL - Financial data for alpha research.

A Python package for fetching financial data from S3, returning wide tables
optimized for alpha research, with local caching and composable operators.

Example:
    ```python
    from quantdl import QuantDLClient

    client = QuantDLClient()

    # Get daily closing prices as wide table
    prices = client.daily(["AAPL", "MSFT", "GOOGL"], "close", "2024-01-01", "2024-12-31")

    # Apply operators
    from quantdl.operators import ts_mean, rank

    ma_20 = ts_mean(prices, 20)
    ranked = rank(ma_20)
    ```
"""

from quantdl.client import QuantDLClient
from quantdl.exceptions import (
    CacheError,
    ConfigurationError,
    DataNotFoundError,
    QuantDLError,
    S3Error,
    SecurityNotFoundError,
)
from quantdl.types import SecurityInfo

__version__ = "0.1.1"

__all__ = [
    "QuantDLClient",
    "SecurityInfo",
    # Exceptions
    "QuantDLError",
    "SecurityNotFoundError",
    "DataNotFoundError",
    "S3Error",
    "CacheError",
    "ConfigurationError",
]
