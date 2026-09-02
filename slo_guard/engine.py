"""Orchestration for memory-efficient streaming reliability analysis."""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from math import ceil

from .buffering import EventTimeBuffer
from .config import GuardConfig
from .detectors import (
    BurnRateResult,
    MultiWindowBurnRateDetector,
    PageHinkleyDetector,
    PageHinkleyResult,
)
from .models import Alert, TelemetryEvent
from .sketches import GKQuantileSketch, HeavyHitters


@dataclass(frozen=True, slots=True)
class IngestResult:
    """State transition produced by one ingestion attempt."""

    accepted: bool
    processed: int
    alerts: tuple[Alert, ...]
    dropped_reason: str | None = None


@dataclass(slots=True)
class _ServiceState:
    latency: GKQuantileSketch
    errors: HeavyHitters
    drift: PageHinkleyDetector
    burn: MultiWindowBurnRateDetector
    requests: int = 0
    error_count: int = 0
    latency_changes: int = 0
    latest_burn: BurnRateResult | None = None
    latest_change: PageHinkleyResult | None = None
    last_alert_at: dict[str, float] = field(default_factory=dict)


class StreamingSLOGuard:
    """Analyze request telemetry with cardinality-controlled in-memory state.

    ``ingest`` accepts events in arrival order. Events that are within the
    configured lateness bound are processed in event-time order. Call ``flush``
    at the end of a finite stream so its remaining tail is included.
    """

    def __init__(self, config: GuardConfig | None = None) -> None:
        self.config = config or GuardConfig()
        self.buffer = EventTimeBuffer(
            allowed_lateness=self.config.allowed_lateness_seconds,
            max_buffer_events=self.config.max_buffer_events,
            dedupe_capacity=self.config.dedupe_capacity,
        )
        self._services: dict[str, _ServiceState] = {}
        self._reserved_services: set[str] = set()
        self._alerts: deque[Alert] = deque(maxlen=self.config.max_retained_alerts)
        self._processed_events = 0
        self._processed_errors = 0
        self._emitted_alerts = 0
        self._dropped_service_capacity = 0
        self._latest_event_timestamp: float | None = None

    @property
    def processed_events(self) -> int:
        return self._processed_events

    @property
    def tracked_services(self) -> int:
        return len(self._reserved_services)

    def ingest(self, event: TelemetryEvent) -> IngestResult:
        """Accept one validated event and process any watermark-ready records."""

        if not isinstance(event, TelemetryEvent):
            raise TypeError("event must be a TelemetryEvent")

        is_new_service = event.service not in self._reserved_services
        if is_new_service and len(self._reserved_services) >= self.config.max_services:
            self._dropped_service_capacity += 1
            return IngestResult(
                accepted=False,
                processed=0,
                alerts=(),
                dropped_reason="service_capacity_exceeded",
            )

        outcome = self.buffer.add(event)
        if outcome.accepted and is_new_service:
            self._reserved_services.add(event.service)

        emitted_alerts = self._process_many(outcome.emitted)
        return IngestResult(
            accepted=outcome.accepted,
            processed=len(outcome.emitted),
            alerts=emitted_alerts,
            dropped_reason=outcome.dropped_reason,
        )

    def flush(self) -> tuple[Alert, ...]:
        """Process all buffered events in event-time order."""

        return self._process_many(self.buffer.flush())

    def snapshot(self) -> dict[str, object]:
        """Return a JSON-serializable explanation of current state."""

        services = {
            name: self._service_snapshot(state)
            for name, state in sorted(self._services.items())
        }
        buffer_metrics = self.buffer.metrics()
        buffer_metrics["dropped_service_capacity"] = self._dropped_service_capacity
        return {
            "schema_version": "1.0",
            "latest_event_timestamp": self._latest_event_timestamp,
            "config": self.config.to_dict(),
            "ingestion": buffer_metrics,
            "totals": {
                "processed_events": self._processed_events,
                "processed_errors": self._processed_errors,
                "tracked_services": self.tracked_services,
                "emitted_alerts": self._emitted_alerts,
                "retained_alerts": len(self._alerts),
            },
            "services": services,
            "alerts": [alert.to_dict() for alert in self._alerts],
        }

    def _new_service_state(self) -> _ServiceState:
        return _ServiceState(
            latency=GKQuantileSketch(self.config.quantile_epsilon),
            errors=HeavyHitters(
                self.config.heavy_hitter_capacity,
                width=self.config.count_min_width,
                depth=self.config.count_min_depth,
                seed=0,
            ),
            drift=PageHinkleyDetector(
                warmup=self.config.drift_warmup,
                delta=self.config.drift_delta,
                threshold=self.config.drift_threshold,
                alpha=self.config.drift_alpha,
            ),
            burn=MultiWindowBurnRateDetector(
                short_window=self.config.short_window_seconds,
                long_window=self.config.long_window_seconds,
                slo_target=self.config.slo_target,
                fast_burn_threshold=self.config.fast_burn_threshold,
                slow_burn_threshold=self.config.slow_burn_threshold,
                minimum_traffic=self.config.minimum_window_traffic,
                max_samples=self.config.max_window_samples,
                late_timestamp_policy="reject",
            ),
        )

    def _process_many(self, events: tuple[TelemetryEvent, ...]) -> tuple[Alert, ...]:
        emitted: list[Alert] = []
        for event in events:
            emitted.extend(self._process(event))
        return tuple(emitted)

    def _process(self, event: TelemetryEvent) -> list[Alert]:
        state = self._services.get(event.service)
        if state is None:
            state = self._new_service_state()
            self._services[event.service] = state

        state.requests += 1
        state.latency.insert(event.latency_ms)
        if event.is_error:
            state.error_count += 1
            self._processed_errors += 1
            signature = event.error_signature or f"http-{event.status_code}"
            state.errors.update(signature)

        state.latest_burn = state.burn.update(
            event.timestamp, requests=1, errors=int(event.is_error)
        )
        change = state.drift.update(event.latency_ms)
        state.latest_change = change

        self._processed_events += 1
        self._latest_event_timestamp = event.timestamp

        emitted: list[Alert] = []
        if state.latest_burn.alert and self._outside_cooldown(
            state, "error_budget_burn", event.timestamp
        ):
            burn_alert = self._burn_alert(event, state.latest_burn)
            emitted.append(burn_alert)
            self._record_alert(state, burn_alert)

        if change.change_detected:
            state.latency_changes += 1
            if self._outside_cooldown(state, "latency_change", event.timestamp):
                change_alert = self._change_alert(event, change)
                emitted.append(change_alert)
                self._record_alert(state, change_alert)
            # A detected regime shift becomes the baseline for the next change.
            state.drift.reset()

        return emitted

    def _outside_cooldown(
        self, state: _ServiceState, alert_type: str, timestamp: float
    ) -> bool:
        last = state.last_alert_at.get(alert_type)
        return last is None or timestamp - last >= self.config.alert_cooldown_seconds

    def _record_alert(self, state: _ServiceState, alert: Alert) -> None:
        state.last_alert_at[alert.alert_type] = alert.timestamp
        self._alerts.append(alert)
        self._emitted_alerts += 1

    @staticmethod
    def _burn_alert(event: TelemetryEvent, result: BurnRateResult) -> Alert:
        severity = "critical" if result.fast_alert else "warning"
        return Alert(
            alert_type="error_budget_burn",
            service=event.service,
            timestamp=event.timestamp,
            severity=severity,
            message=(
                f"{event.service} is consuming its error budget at a "
                f"{result.alert_kind.replace('_', ' ')} rate"
            ),
            evidence={
                "alert_kind": result.alert_kind,
                "slo_target": result.slo_target,
                "short_window_seconds": result.short.duration,
                "short_requests": result.short.requests,
                "short_errors": result.short.errors,
                "short_burn_rate": result.short.burn_rate,
                "short_threshold": result.short.threshold,
                "long_window_seconds": result.long.duration,
                "long_requests": result.long.requests,
                "long_errors": result.long.errors,
                "long_burn_rate": result.long.burn_rate,
                "long_threshold": result.long.threshold,
            },
        )

    @staticmethod
    def _change_alert(event: TelemetryEvent, result: PageHinkleyResult) -> Alert:
        return Alert(
            alert_type="latency_change",
            service=event.service,
            timestamp=event.timestamp,
            severity="warning",
            message=f"{event.service} latency moved above its learned baseline",
            evidence={
                "observation_ms": result.value,
                "mean_ms": result.mean,
                "deviation": result.deviation,
                "threshold": result.threshold,
                "observations_since_reset": result.count,
            },
        )

    def _service_snapshot(self, state: _ServiceState) -> dict[str, object]:
        quantiles = {
            "p50": state.latency.query(0.50),
            "p95": state.latency.query(0.95),
            "p99": state.latency.query(0.99),
            "observations": state.latency.count,
            "summary_tuples": state.latency.size,
            "rank_error_bound": ceil(state.latency.epsilon * state.latency.count),
        }
        error_signatures = [
            {
                "key": item.key,
                "estimated_count": item.estimate,
                "error_bound": item.error,
                "lower_bound": item.lower_bound,
            }
            for item in state.errors.top_k()
        ]
        burn = None
        if state.latest_burn is not None:
            burn = {
                "alert": state.latest_burn.alert,
                "alert_kind": state.latest_burn.alert_kind,
                "short": asdict(state.latest_burn.short),
                "long": asdict(state.latest_burn.long),
            }
        latest_change = None
        if state.latest_change is not None:
            latest_change = {
                "mean_ms": state.latest_change.mean,
                "deviation": state.latest_change.deviation,
                "threshold": state.latest_change.threshold,
                "is_warm": state.latest_change.is_warm,
                "change_detected": state.latest_change.change_detected,
            }
        return {
            "requests": state.requests,
            "errors": state.error_count,
            "error_rate": state.error_count / state.requests,
            "latency_ms": quantiles,
            "error_signatures": error_signatures,
            "burn_rate": burn,
            "latency_change": {
                "detections": state.latency_changes,
                "latest_evaluation": latest_change,
            },
        }
