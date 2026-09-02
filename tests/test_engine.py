import pytest

from slo_guard.config import GuardConfig
from slo_guard.engine import StreamingSLOGuard
from slo_guard.models import TelemetryEvent


def event(
    event_id: str,
    timestamp: float,
    *,
    service: str = "checkout",
    latency: float = 20,
    status: int = 200,
    signature: str | None = None,
) -> TelemetryEvent:
    return TelemetryEvent(
        event_id=event_id,
        timestamp=timestamp,
        service=service,
        latency_ms=latency,
        status_code=status,
        error_signature=signature,
    )


def test_guard_reports_quantiles_and_error_signatures() -> None:
    guard = StreamingSLOGuard(
        GuardConfig(allowed_lateness_seconds=0, minimum_window_traffic=50)
    )

    guard.ingest(event("a", 1, latency=10))
    guard.ingest(
        event(
            "b",
            2,
            latency=30,
            status=503,
            signature="payment-timeout",
        )
    )
    guard.ingest(
        event(
            "c",
            3,
            latency=20,
            status=500,
            signature="payment-timeout",
        )
    )

    report = guard.snapshot()
    checkout = report["services"]["checkout"]  # type: ignore[index]
    assert checkout["requests"] == 3
    assert checkout["errors"] == 2
    assert checkout["latency_ms"]["p50"] == 20  # type: ignore[index]
    assert checkout["error_signatures"][0]["key"] == "payment-timeout"  # type: ignore[index]
    assert checkout["error_signatures"][0]["estimated_count"] == 2  # type: ignore[index]


def test_guard_reorders_then_flushes_event_time_tail() -> None:
    guard = StreamingSLOGuard(GuardConfig(allowed_lateness_seconds=2))

    first = guard.ingest(event("a", 10))
    second = guard.ingest(event("b", 9))
    third = guard.ingest(event("c", 13))

    assert first.processed == 0
    assert second.processed == 0
    assert third.processed == 2
    assert guard.processed_events == 2
    guard.flush()
    assert guard.processed_events == 3
    assert guard.snapshot()["latest_event_timestamp"] == 13


def test_guard_emits_evidence_bearing_burn_alert() -> None:
    config = GuardConfig(
        allowed_lateness_seconds=0,
        slo_target=0.9,
        short_window_seconds=5,
        long_window_seconds=10,
        fast_burn_threshold=2,
        slow_burn_threshold=1,
        minimum_window_traffic=2,
        alert_cooldown_seconds=60,
    )
    guard = StreamingSLOGuard(config)

    guard.ingest(event("ok", 1))
    result = guard.ingest(event("bad", 2, status=503, signature="upstream-timeout"))

    assert len(result.alerts) == 1
    alert = result.alerts[0]
    assert alert.alert_type == "error_budget_burn"
    assert alert.severity == "critical"
    assert alert.evidence["short_burn_rate"] == pytest.approx(5)
    assert alert.evidence["short_requests"] == 2


def test_guard_applies_alert_cooldown_to_an_ongoing_burn() -> None:
    guard = StreamingSLOGuard(
        GuardConfig(
            allowed_lateness_seconds=0,
            slo_target=0.99,
            short_window_seconds=5,
            long_window_seconds=10,
            minimum_window_traffic=1,
            alert_cooldown_seconds=10,
        )
    )

    first = guard.ingest(event("bad-1", 1, status=503))
    suppressed = guard.ingest(event("bad-2", 2, status=503))
    repeated = guard.ingest(event("bad-3", 11, status=503))

    assert len(first.alerts) == 1
    assert suppressed.alerts == ()
    assert len(repeated.alerts) == 1


def test_guard_detects_latency_regime_change_and_resets_detector() -> None:
    config = GuardConfig(
        allowed_lateness_seconds=0,
        minimum_window_traffic=100,
        drift_warmup=3,
        drift_threshold=10,
        drift_alpha=1,
    )
    guard = StreamingSLOGuard(config)

    for index in range(3):
        guard.ingest(event(f"base-{index}", index, latency=10))
    result = guard.ingest(event("shift", 3, latency=100))

    assert [alert.alert_type for alert in result.alerts] == ["latency_change"]
    assert result.alerts[0].evidence["mean_ms"] == 32.5
    service = guard.snapshot()["services"]["checkout"]  # type: ignore[index]
    assert service["latency_change"]["detections"] == 1  # type: ignore[index]


def test_guard_caps_service_cardinality_before_buffering() -> None:
    guard = StreamingSLOGuard(GuardConfig(max_services=1, allowed_lateness_seconds=100))

    assert guard.ingest(event("a", 1, service="checkout")).accepted
    rejected = guard.ingest(event("b", 2, service="catalog"))

    assert not rejected.accepted
    assert rejected.dropped_reason == "service_capacity_exceeded"
    report = guard.snapshot()
    assert report["totals"]["tracked_services"] == 1  # type: ignore[index]
    assert report["ingestion"]["dropped_service_capacity"] == 1  # type: ignore[index]


def test_guard_suppresses_duplicate_ids() -> None:
    guard = StreamingSLOGuard(GuardConfig(allowed_lateness_seconds=0))

    guard.ingest(event("same", 1))
    duplicate = guard.ingest(event("same", 2))

    assert not duplicate.accepted
    assert duplicate.dropped_reason == "duplicate_event"
    assert guard.processed_events == 1
