"""Validated telemetry and report models for the streaming SLO guard."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from math import isfinite
from typing import Any

MAX_EVENT_ID_LENGTH = 128
MAX_SERVICE_LENGTH = 128
MAX_ERROR_SIGNATURE_LENGTH = 256
MAX_LATENCY_MS = 3_600_000.0


class TelemetryValidationError(ValueError):
    """Raised when an input record violates the public telemetry contract."""


def _bounded_text(value: object, field_name: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise TelemetryValidationError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise TelemetryValidationError(f"{field_name} must not be empty")
    if len(normalized) > maximum:
        raise TelemetryValidationError(
            f"{field_name} must contain at most {maximum} characters"
        )
    return normalized


def _finite_number(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TelemetryValidationError(f"{field_name} must be a finite number")
    number = float(value)
    if not isfinite(number):
        raise TelemetryValidationError(f"{field_name} must be a finite number")
    return number


@dataclass(frozen=True, slots=True)
class TelemetryEvent:
    """One request-level telemetry observation.

    Timestamps are Unix seconds. The guard considers HTTP 5xx responses errors;
    4xx responses remain caller-visible failures but do not consume the service's
    reliability budget by default.
    """

    event_id: str
    timestamp: float
    service: str
    latency_ms: float
    status_code: int
    error_signature: str | None = None

    def __post_init__(self) -> None:
        """Enforce the contract even when callers bypass ``from_mapping``."""

        event_id = _bounded_text(self.event_id, "event_id", MAX_EVENT_ID_LENGTH)
        service = _bounded_text(self.service, "service", MAX_SERVICE_LENGTH)
        timestamp = _finite_number(self.timestamp, "timestamp")
        if timestamp < 0:
            raise TelemetryValidationError("timestamp must be non-negative")
        latency_ms = _finite_number(self.latency_ms, "latency_ms")
        if not 0 <= latency_ms <= MAX_LATENCY_MS:
            raise TelemetryValidationError(
                f"latency_ms must be between 0 and {MAX_LATENCY_MS:g}"
            )
        if isinstance(self.status_code, bool) or not isinstance(self.status_code, int):
            raise TelemetryValidationError("status_code must be an integer")
        if not 100 <= self.status_code <= 599:
            raise TelemetryValidationError("status_code must be between 100 and 599")
        if self.error_signature is None:
            error_signature = None
        else:
            error_signature = _bounded_text(
                self.error_signature,
                "error_signature",
                MAX_ERROR_SIGNATURE_LENGTH,
            )

        object.__setattr__(self, "event_id", event_id)
        object.__setattr__(self, "service", service)
        object.__setattr__(self, "timestamp", timestamp)
        object.__setattr__(self, "latency_ms", latency_ms)
        object.__setattr__(self, "error_signature", error_signature)

    @property
    def is_error(self) -> bool:
        return self.status_code >= 500

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> TelemetryEvent:
        if not isinstance(payload, Mapping):
            raise TelemetryValidationError("telemetry record must be a JSON object")

        event_id = _bounded_text(
            payload.get("event_id"), "event_id", MAX_EVENT_ID_LENGTH
        )
        service = _bounded_text(payload.get("service"), "service", MAX_SERVICE_LENGTH)

        timestamp = _finite_number(payload.get("timestamp"), "timestamp")
        if timestamp < 0:
            raise TelemetryValidationError("timestamp must be non-negative")

        latency_ms = _finite_number(payload.get("latency_ms"), "latency_ms")
        if not 0 <= latency_ms <= MAX_LATENCY_MS:
            raise TelemetryValidationError(
                f"latency_ms must be between 0 and {MAX_LATENCY_MS:g}"
            )

        status_code = payload.get("status_code")
        if isinstance(status_code, bool) or not isinstance(status_code, int):
            raise TelemetryValidationError("status_code must be an integer")
        if not 100 <= status_code <= 599:
            raise TelemetryValidationError("status_code must be between 100 and 599")

        raw_signature = payload.get("error_signature")
        if raw_signature is None:
            error_signature = None
        else:
            error_signature = _bounded_text(
                raw_signature, "error_signature", MAX_ERROR_SIGNATURE_LENGTH
            )

        return cls(
            event_id=event_id,
            timestamp=timestamp,
            service=service,
            latency_ms=latency_ms,
            status_code=status_code,
            error_signature=error_signature,
        )

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "service": self.service,
            "latency_ms": self.latency_ms,
            "status_code": self.status_code,
        }
        if self.error_signature is not None:
            result["error_signature"] = self.error_signature
        return result


@dataclass(frozen=True, slots=True)
class Alert:
    """An evidence-bearing signal emitted by the guard."""

    alert_type: str
    service: str
    timestamp: float
    severity: str
    message: str
    evidence: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "type": self.alert_type,
            "service": self.service,
            "timestamp": self.timestamp,
            "severity": self.severity,
            "message": self.message,
            "evidence": dict(self.evidence),
        }
