"""Storage backends for QuantDL."""

from quantdl.storage.backend import S3RequestCounter, StorageBackend
from quantdl.storage.cache import DiskCache

# Backward compatibility alias
S3StorageBackend = StorageBackend

__all__ = ["DiskCache", "StorageBackend", "S3StorageBackend", "S3RequestCounter"]
