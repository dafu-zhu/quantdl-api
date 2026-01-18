"""Type definitions for alpha expressions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Union

if TYPE_CHECKING:
    from quantdl.alpha.core import Alpha

import polars as pl

# Core type for values that can participate in alpha operations
AlphaLike = Union["Alpha", pl.DataFrame, int, float]

# Scalar types for broadcasting
Scalar = Union[int, float]  # noqa: UP007

# Data source types
DataSourceType = Literal["ticks", "fundamentals", "metrics"]


@dataclass(frozen=True, slots=True)
class DataSpec:
    """Specification for data field resolution.

    Defines how to fetch a named field from the QuantDLClient.

    Attributes:
        source: Data source type (ticks, fundamentals, metrics)
        field: Field name within the source (e.g., "close", "Revenue", "pe_ratio")
    """

    source: DataSourceType
    field: str
