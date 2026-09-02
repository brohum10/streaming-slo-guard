import math

import pytest

from slo_guard.buffering import EventTimeBuffer
from slo_guard.models import TelemetryEvent, TelemetryValidationError


def event(
    event_id: str, timestamp: float, *, service: str = "checkout"
) -> TelemetryEvent:
    return TelemetryEvent(
        event_id=event_id,
        timestamp=timestamp,
        service=service,
        latency_ms=25.0,
        status_code=200,
    )


def test_event_validation_and_error_classification() -> None:
    valid = TelemetryEvent.from_mapping(
        {
            "event_id": " req-1 ",
            "timestamp": 1_700_000_000,
            "service": " checkout ",
            "latency_ms": 42.5,
            "status_code": 503,
            "error_signature": " upstream-timeout ",
        }
    )

    assert valid.event_id == "req-1"
    assert valid.service == "checkout"
    assert valid.error_signature == "upstream-timeout"
    assert valid.is_error
    assert not TelemetryEvent.from_mapping(
        {
            "event_id": "req-2",
            "timestamp": 1,
            "service": "checkout",
            "latency_ms": 4,
            "status_code": 429,
        }
    ).is_error


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", ""),
        ("timestamp", math.inf),
        ("timestamp", -1),
        ("latency_ms", -0.1),
        ("status_code", 99),
        ("status_code", True),
    ],
)
def test_event_rejects_invalid_fields(field: str, value: object) -> None:
    payload: dict[str, object] = {
        "event_id": "req-1",
        "timestamp": 1,
        "service": "checkout",
        "latency_ms": 20,
        "status_code": 200,
    }
    payload[field] = value

    with pytest.raises(TelemetryValidationError):
        TelemetryEvent.from_mapping(payload)


def test_direct_event_construction_enforces_and_normalizes_the_contract() -> None:
    normalized = TelemetryEvent(
        event_id=" req-1 ",
        timestamp=1,
        service=" checkout ",
        latency_ms=20,
        status_code=200,
    )

    assert normalized.event_id == "req-1"
    assert normalized.service == "checkout"
    assert normalized.timestamp == 1.0
    with pytest.raises(TelemetryValidationError):
        TelemetryEvent(
            event_id="",
            timestamp=1,
            service="checkout",
            latency_ms=20,
            status_code=200,
        )


def test_event_time_buffer_reorders_within_watermark() -> None:
    buffer = EventTimeBuffer(allowed_lateness=2)

    assert buffer.add(event("a", 10)).emitted == ()
    assert buffer.add(event("b", 9)).emitted == ()
    outcome = buffer.add(event("c", 13))

    assert [item.event_id for item in outcome.emitted] == ["b", "a"]
    assert [item.event_id for item in buffer.flush()] == ["c"]


def test_event_time_buffer_drops_duplicates_and_irrecoverably_late_events() -> None:
    buffer = EventTimeBuffer(allowed_lateness=2)

    buffer.add(event("a", 10))
    duplicate = buffer.add(event("a", 11))
    buffer.add(event("b", 14))
    late = buffer.add(event("old", 9))

    assert not duplicate.accepted
    assert duplicate.dropped_reason == "duplicate_event"
    assert not late.accepted
    assert late.dropped_reason == "beyond_watermark"
    assert buffer.metrics()["dropped_duplicate"] == 1
    assert buffer.metrics()["dropped_late"] == 1


def test_event_time_buffer_caps_dedupe_memory() -> None:
    buffer = EventTimeBuffer(allowed_lateness=100, dedupe_capacity=2)
    buffer.add(event("a", 1))
    buffer.add(event("b", 2))
    buffer.add(event("c", 3))

    # The oldest ID is evicted, making a later retry eligible for processing.
    assert buffer.add(event("a", 4)).accepted


def test_event_time_buffer_enforces_capacity() -> None:
    buffer = EventTimeBuffer(allowed_lateness=100, max_buffer_events=2)

    buffer.add(event("a", 1))
    buffer.add(event("b", 2))
    outcome = buffer.add(event("c", 3))

    assert not outcome.accepted
    assert outcome.dropped_reason == "buffer_capacity_exceeded"
    assert buffer.buffered == 2


@pytest.mark.parametrize(
    "kwargs",
    [
        {"allowed_lateness": math.nan},
        {"allowed_lateness": True},
        {"max_buffer_events": 1.5},
        {"dedupe_capacity": False},
    ],
)
def test_event_time_buffer_rejects_invalid_limits(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        EventTimeBuffer(**kwargs)  # type: ignore[arg-type]
