"""Alpha expression DSL for composing operators.

Example:
    >>> from quantdl.alpha import Alpha, AlphaSession, alpha_eval
    >>> import quantdl.operators as ops
    >>>
    >>> # Using AlphaSession for automatic data fetching
    >>> with AlphaSession(client, ["AAPL", "MSFT"], "2024-01-01", "2024-12-31") as s:
    ...     close = s.close   # Lazy fetch, returns Alpha
    ...     signal = close / Alpha(ops.ts_delay(close.data, 1)) - 1
    >>>
    >>> # Using Alpha class with operator overloading
    >>> close = Alpha(close_df)
    >>> volume = Alpha(volume_df)
    >>> alpha = ops.rank(-ops.ts_delta(close.data, 5))
    >>>
    >>> # Using string DSL
    >>> result = alpha_eval(
    ...     "ops.rank(-ops.ts_delta(close, 5))",
    ...     {"close": close_df},
    ...     ops=ops,
    ... )
"""

from quantdl.alpha.core import Alpha
from quantdl.alpha.parser import AlphaParseError, alpha_eval
from quantdl.alpha.session import AlphaSession
from quantdl.alpha.types import AlphaLike, DataSpec, Scalar
from quantdl.alpha.validation import (
    AlphaError,
    AlphaSessionError,
    ColumnMismatchError,
    DateMismatchError,
    FieldNotFoundError,
    SessionNotActiveError,
)

__all__ = [
    # Core
    "Alpha",
    "AlphaSession",
    "alpha_eval",
    # Types
    "AlphaLike",
    "DataSpec",
    "Scalar",
    # Exceptions
    "AlphaError",
    "AlphaParseError",
    "AlphaSessionError",
    "ColumnMismatchError",
    "DateMismatchError",
    "FieldNotFoundError",
    "SessionNotActiveError",
]
