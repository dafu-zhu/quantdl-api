"""Custom exceptions for QuantDL."""


class QuantDLError(Exception):
    """Base exception for QuantDL errors."""


class SecurityNotFoundError(QuantDLError):
    """Raised when a security cannot be resolved."""

    def __init__(self, identifier: str, as_of: str | None = None) -> None:
        self.identifier = identifier
        self.as_of = as_of
        msg = f"Security not found: {identifier}"
        if as_of:
            msg += f" as of {as_of}"
        super().__init__(msg)


class DataNotFoundError(QuantDLError):
    """Raised when requested data is not available."""

    def __init__(self, data_type: str, identifier: str) -> None:
        self.data_type = data_type
        self.identifier = identifier
        super().__init__(f"{data_type} data not found for: {identifier}")


class S3Error(QuantDLError):
    """Raised when S3 operations fail."""

    def __init__(self, operation: str, path: str, cause: Exception | None = None) -> None:
        self.operation = operation
        self.path = path
        self.cause = cause
        msg = f"S3 {operation} failed for: {path}"
        if cause:
            msg += f" ({cause})"
        super().__init__(msg)


class CacheError(QuantDLError):
    """Raised when cache operations fail."""


class ConfigurationError(QuantDLError):
    """Raised when configuration is invalid or missing."""
