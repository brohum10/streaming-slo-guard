"""Streaming SLO Guard public API."""

from .config import GuardConfig
from .engine import IngestResult, StreamingSLOGuard
from .models import Alert, TelemetryEvent, TelemetryValidationError

__all__ = [
    "Alert",
    "GuardConfig",
    "IngestResult",
    "StreamingSLOGuard",
    "TelemetryEvent",
    "TelemetryValidationError",
]

__version__ = "0.1.0"
