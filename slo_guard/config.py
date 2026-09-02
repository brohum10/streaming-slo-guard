"""Configuration for the streaming SLO guard."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import isfinite
from numbers import Real


@dataclass(frozen=True, slots=True)
class GuardConfig:
    """Validated limits and detector settings.

    Defaults favor an interactive demonstration. Production deployments should
    tune thresholds from measured service behavior and their actual SLO policy.
    """

    slo_target: float = 0.999
    short_window_seconds: float = 300.0
    long_window_seconds: float = 3_600.0
    fast_burn_threshold: float = 14.4
    slow_burn_threshold: float = 6.0
    minimum_window_traffic: int = 25
    max_window_samples: int = 10_000

    quantile_epsilon: float = 0.01
    heavy_hitter_capacity: int = 8
    count_min_width: int = 512
    count_min_depth: int = 5

    drift_warmup: int = 30
    drift_delta: float = 0.005
    drift_threshold: float = 50.0
    drift_alpha: float = 0.999

    allowed_lateness_seconds: float = 5.0
    max_buffer_events: int = 10_000
    dedupe_capacity: int = 100_000
    max_services: int = 256
    alert_cooldown_seconds: float = 60.0
    max_retained_alerts: int = 1_000

    def __post_init__(self) -> None:
        self._fraction("slo_target", self.slo_target)
        self._positive("short_window_seconds", self.short_window_seconds)
        self._positive("long_window_seconds", self.long_window_seconds)
        if self.short_window_seconds >= self.long_window_seconds:
            raise ValueError(
                "short_window_seconds must be less than long_window_seconds"
            )
        self._positive("fast_burn_threshold", self.fast_burn_threshold)
        self._positive("slow_burn_threshold", self.slow_burn_threshold)
        if self.fast_burn_threshold < self.slow_burn_threshold:
            raise ValueError(
                "fast_burn_threshold cannot be less than slow_burn_threshold"
            )
        self._positive_int("minimum_window_traffic", self.minimum_window_traffic)
        self._positive_int("max_window_samples", self.max_window_samples)
        if self.max_window_samples < self.minimum_window_traffic:
            raise ValueError(
                "max_window_samples cannot be smaller than minimum_window_traffic"
            )

        if (
            isinstance(self.quantile_epsilon, bool)
            or not isinstance(self.quantile_epsilon, Real)
            or not isfinite(self.quantile_epsilon)
            or not 0 < self.quantile_epsilon < 0.5
        ):
            raise ValueError("quantile_epsilon must be between 0 and 0.5")
        self._positive_int("heavy_hitter_capacity", self.heavy_hitter_capacity)
        self._positive_int("count_min_width", self.count_min_width)
        self._positive_int("count_min_depth", self.count_min_depth)

        self._positive_int("drift_warmup", self.drift_warmup)
        self._non_negative("drift_delta", self.drift_delta)
        self._positive("drift_threshold", self.drift_threshold)
        if (
            isinstance(self.drift_alpha, bool)
            or not isinstance(self.drift_alpha, Real)
            or not isfinite(self.drift_alpha)
            or not 0 < self.drift_alpha <= 1
        ):
            raise ValueError("drift_alpha must be in (0, 1]")

        self._non_negative("allowed_lateness_seconds", self.allowed_lateness_seconds)
        self._positive_int("max_buffer_events", self.max_buffer_events)
        self._positive_int("dedupe_capacity", self.dedupe_capacity)
        self._positive_int("max_services", self.max_services)
        self._non_negative("alert_cooldown_seconds", self.alert_cooldown_seconds)
        self._positive_int("max_retained_alerts", self.max_retained_alerts)

    def to_dict(self) -> dict[str, float | int]:
        return asdict(self)

    @staticmethod
    def _fraction(name: str, value: float) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, Real)
            or not isfinite(value)
            or not 0 < value < 1
        ):
            raise ValueError(f"{name} must be a finite value between 0 and 1")

    @staticmethod
    def _positive(name: str, value: float) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, Real)
            or not isfinite(value)
            or value <= 0
        ):
            raise ValueError(f"{name} must be a positive finite value")

    @staticmethod
    def _non_negative(name: str, value: float) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, Real)
            or not isfinite(value)
            or value < 0
        ):
            raise ValueError(f"{name} must be a non-negative finite value")

    @staticmethod
    def _positive_int(name: str, value: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
