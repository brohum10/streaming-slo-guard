"""Streaming detectors for change points and error-budget consumption.

The implementations in this module intentionally depend only on the Python
standard library.  Every call updates a small amount of state and returns an
immutable explanation of the decision that was made.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from numbers import Integral, Real
from typing import Literal

LateTimestampPolicy = Literal["reject", "ignore"]
AlertKind = Literal["none", "fast", "slow", "fast_and_slow"]


def _finite_real(name: str, value: Real) -> float:
    """Return *value* as a float, rejecting booleans and non-finite values."""

    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{name} must be a real number, got {type(value).__name__}")
    converted = float(value)
    if not math.isfinite(converted):
        raise ValueError(f"{name} must be finite")
    return converted


def _integer(name: str, value: int, *, minimum: int = 0) -> int:
    """Return a validated integer without silently accepting ``bool``."""

    if isinstance(value, bool) or not isinstance(value, Integral):
        raise TypeError(f"{name} must be an integer, got {type(value).__name__}")
    converted = int(value)
    if converted < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    return converted


@dataclass(frozen=True, slots=True)
class PageHinkleyResult:
    """Explanation returned for one Page-Hinkley observation.

    ``deviation`` is the distance between the current cumulative statistic and
    its running minimum.  A change is reported only after ``warmup`` samples and
    only when that distance is strictly greater than ``threshold``.
    """

    value: float
    count: int
    mean: float
    cumulative_sum: float
    minimum_cumulative_sum: float
    deviation: float
    threshold: float
    is_warm: bool
    change_detected: bool

    @property
    def cumulative(self) -> float:
        """Alias for :attr:`cumulative_sum`."""

        return self.cumulative_sum

    @property
    def detected(self) -> bool:
        """Alias for :attr:`change_detected`."""

        return self.change_detected


class PageHinkleyDetector:
    """Detect sustained upward changes in the mean of a numeric stream.

    The running mean is the exact arithmetic mean of all observations since the
    last reset.  For observation ``x`` the cumulative Page-Hinkley statistic is
    updated as::

        cumulative = alpha * cumulative + (x - mean - delta)

    ``alpha`` is therefore a forgetting factor for historical cumulative
    evidence: ``1.0`` applies no forgetting and smaller values discount it more
    quickly.  The detector reports an upward change when the statistic has risen
    more than ``threshold`` above its historical minimum.  Detection does not
    implicitly reset state; callers can inspect the explanation and choose when
    to call :meth:`reset`.
    """

    def __init__(
        self,
        *,
        warmup: int = 30,
        delta: float = 0.005,
        threshold: float = 50.0,
        alpha: float = 1.0,
    ) -> None:
        self.warmup = _integer("warmup", warmup, minimum=1)
        self.delta = _finite_real("delta", delta)
        self.threshold = _finite_real("threshold", threshold)
        self.alpha = _finite_real("alpha", alpha)

        if self.delta < 0.0:
            raise ValueError("delta must be non-negative")
        if self.threshold <= 0.0:
            raise ValueError("threshold must be greater than zero")
        if not 0.0 < self.alpha <= 1.0:
            raise ValueError("alpha must be in the interval (0, 1]")

        self.reset()

    @property
    def count(self) -> int:
        """Number of observations seen since the last reset."""

        return self._count

    @property
    def mean(self) -> float:
        """Current arithmetic mean, or ``0.0`` before the first observation."""

        return self._mean

    @property
    def cumulative_sum(self) -> float:
        """Current discounted cumulative statistic."""

        return self._cumulative_sum

    @property
    def deviation(self) -> float:
        """Current distance from the running minimum cumulative statistic."""

        return self._cumulative_sum - self._minimum_cumulative_sum

    @property
    def forgetting_factor(self) -> float:
        """Descriptive alias for :attr:`alpha`."""

        return self.alpha

    def reset(self) -> None:
        """Return the detector to its initial state without changing config."""

        self._count = 0
        self._mean = 0.0
        self._cumulative_sum = 0.0
        self._minimum_cumulative_sum = 0.0

    def update(self, value: float) -> PageHinkleyResult:
        """Consume one finite observation and return the resulting explanation."""

        observation = _finite_real("value", value)

        # Compute into locals first so a numerically unrepresentable update does
        # not leave half-mutated detector state.  The weighted form of the mean
        # also avoids overflowing on a transition between very large values of
        # opposite sign.
        new_count = self._count + 1
        previous_weight = self._count / new_count
        new_mean = self._mean * previous_weight + observation / new_count
        new_cumulative_sum = (
            self.alpha * self._cumulative_sum + observation - new_mean - self.delta
        )
        new_minimum = min(self._minimum_cumulative_sum, new_cumulative_sum)
        new_deviation = new_cumulative_sum - new_minimum
        if not all(
            math.isfinite(number)
            for number in (new_mean, new_cumulative_sum, new_deviation)
        ):
            raise ValueError("value produces a non-finite detector statistic")

        self._count = new_count
        self._mean = new_mean
        self._cumulative_sum = new_cumulative_sum
        self._minimum_cumulative_sum = new_minimum

        is_warm = new_count >= self.warmup
        change_detected = is_warm and new_deviation > self.threshold
        return PageHinkleyResult(
            value=observation,
            count=new_count,
            mean=new_mean,
            cumulative_sum=new_cumulative_sum,
            minimum_cumulative_sum=new_minimum,
            deviation=new_deviation,
            threshold=self.threshold,
            is_warm=is_warm,
            change_detected=change_detected,
        )


@dataclass(frozen=True, slots=True)
class BurnRateWindowResult:
    """Traffic and decision details for one rolling time window."""

    duration: float
    requests: int
    errors: int
    observed_error_rate: float
    burn_rate: float
    threshold: float
    minimum_traffic: int
    enough_traffic: bool
    data_complete: bool
    threshold_exceeded: bool
    alert: bool
    retained_samples: int
    capacity_evictions: int

    @property
    def error_rate(self) -> float:
        """Alias for :attr:`observed_error_rate`."""

        return self.observed_error_rate


@dataclass(frozen=True, slots=True)
class BurnRateResult:
    """Explanation of a multi-window error-budget burn-rate decision."""

    input_timestamp: float
    evaluation_timestamp: float
    sample_accepted: bool
    late_timestamp: bool
    slo_target: float
    error_budget: float
    short: BurnRateWindowResult
    long: BurnRateWindowResult
    fast_alert: bool
    slow_alert: bool
    alert: bool
    alert_kind: AlertKind

    @property
    def timestamp(self) -> float:
        """Timestamp at which the retained state was evaluated."""

        return self.evaluation_timestamp

    @property
    def accepted(self) -> bool:
        """Alias for :attr:`sample_accepted`."""

        return self.sample_accepted

    @property
    def should_alert(self) -> bool:
        """Alias for :attr:`alert`."""

        return self.alert

    @property
    def short_error_rate(self) -> float:
        return self.short.observed_error_rate

    @property
    def long_error_rate(self) -> float:
        return self.long.observed_error_rate

    @property
    def short_burn_rate(self) -> float:
        return self.short.burn_rate

    @property
    def long_burn_rate(self) -> float:
        return self.long.burn_rate

    @property
    def fast_burn_threshold(self) -> float:
        return self.short.threshold

    @property
    def slow_burn_threshold(self) -> float:
        return self.long.threshold

    @property
    def minimum_traffic(self) -> int:
        return self.short.minimum_traffic


@dataclass(frozen=True, slots=True)
class _TrafficSample:
    timestamp: float
    requests: int
    errors: int


class _WindowAccumulator:
    """Rolling exact counters backed by a capacity-bounded deque."""

    def __init__(self, duration: float, max_samples: int) -> None:
        self.duration = duration
        self.max_samples = max_samples
        self.samples: deque[_TrafficSample] = deque()
        self.requests = 0
        self.errors = 0
        self.capacity_evictions = 0
        self._incomplete_until = -math.inf

    def reset(self) -> None:
        self.samples.clear()
        self.requests = 0
        self.errors = 0
        self.capacity_evictions = 0
        self._incomplete_until = -math.inf

    def _remove_left(self) -> _TrafficSample:
        sample = self.samples.popleft()
        self.requests -= sample.requests
        self.errors -= sample.errors
        return sample

    def advance(self, timestamp: float) -> None:
        """Evict samples outside the inclusive ``[now-duration, now]`` window."""

        cutoff = timestamp - self.duration
        while self.samples and self.samples[0].timestamp < cutoff:
            self._remove_left()

    def add(self, sample: _TrafficSample) -> None:
        self.advance(sample.timestamp)

        # A zero-request observation advances the clock but contributes no state.
        if sample.requests == 0:
            return

        if len(self.samples) == self.max_samples:
            dropped = self._remove_left()
            self.capacity_evictions += 1
            self._incomplete_until = max(
                self._incomplete_until, dropped.timestamp + self.duration
            )

        self.samples.append(sample)
        self.requests += sample.requests
        self.errors += sample.errors

    def complete_at(self, timestamp: float) -> bool:
        return timestamp > self._incomplete_until


class MultiWindowBurnRateDetector:
    """Track rapid and sustained error-budget consumption over two windows.

    Error-budget burn rate is defined as ``observed error rate / (1-SLO)``.
    The short window independently raises ``fast_alert`` at
    ``fast_burn_threshold``; the long window raises ``slow_alert`` at
    ``slow_burn_threshold``.  The final alert is their logical OR.  Both checks
    require at least ``minimum_traffic`` requests in their own window.

    Timestamps must be nondecreasing.  A late sample either raises ``ValueError``
    (``late_timestamp_policy='reject'``) or is ignored with an explanatory result
    (``'ignore'``); in both cases detector state is unchanged.

    Each window retains at most ``max_samples`` positive-traffic observations.
    If capacity removes a still-relevant sample, that window is explicitly
    marked incomplete and its alerts are suppressed until the missing sample is
    outside the time window.  This avoids presenting a partial aggregate as an
    exact alert decision.
    """

    def __init__(
        self,
        *,
        short_window: float = 300.0,
        long_window: float = 3_600.0,
        slo_target: float = 0.999,
        fast_burn_threshold: float = 14.4,
        slow_burn_threshold: float = 6.0,
        minimum_traffic: int = 1,
        max_samples: int = 10_000,
        late_timestamp_policy: LateTimestampPolicy = "reject",
    ) -> None:
        self.short_window = _finite_real("short_window", short_window)
        self.long_window = _finite_real("long_window", long_window)
        self.slo_target = _finite_real("slo_target", slo_target)
        self.fast_burn_threshold = _finite_real(
            "fast_burn_threshold", fast_burn_threshold
        )
        self.slow_burn_threshold = _finite_real(
            "slow_burn_threshold", slow_burn_threshold
        )
        self.minimum_traffic = _integer("minimum_traffic", minimum_traffic, minimum=1)
        self.max_samples = _integer("max_samples", max_samples, minimum=1)

        if self.short_window <= 0.0:
            raise ValueError("short_window must be greater than zero")
        if self.long_window <= self.short_window:
            raise ValueError("long_window must be greater than short_window")
        if not 0.0 < self.slo_target < 1.0:
            raise ValueError("slo_target must be in the interval (0, 1)")
        if self.fast_burn_threshold <= 0.0:
            raise ValueError("fast_burn_threshold must be greater than zero")
        if self.slow_burn_threshold <= 0.0:
            raise ValueError("slow_burn_threshold must be greater than zero")
        if self.fast_burn_threshold < self.slow_burn_threshold:
            raise ValueError(
                "fast_burn_threshold must be greater than or equal to "
                "slow_burn_threshold"
            )
        if late_timestamp_policy not in ("reject", "ignore"):
            raise ValueError("late_timestamp_policy must be 'reject' or 'ignore'")
        self.late_timestamp_policy: LateTimestampPolicy = late_timestamp_policy

        self._error_budget = 1.0 - self.slo_target
        self._short = _WindowAccumulator(self.short_window, self.max_samples)
        self._long = _WindowAccumulator(self.long_window, self.max_samples)
        self._last_timestamp: float | None = None

    @property
    def last_timestamp(self) -> float | None:
        """Most recent accepted timestamp, if any."""

        return self._last_timestamp

    @property
    def error_budget(self) -> float:
        """Allowed error fraction, equal to ``1 - slo_target``."""

        return self._error_budget

    @property
    def retained_sample_counts(self) -> tuple[int, int]:
        """Number of retained positive-traffic samples (short, long)."""

        return len(self._short.samples), len(self._long.samples)

    def reset(self) -> None:
        """Clear all observations and timestamp ordering state."""

        self._short.reset()
        self._long.reset()
        self._last_timestamp = None

    def update(self, timestamp: float, requests: int, errors: int) -> BurnRateResult:
        """Consume timestamped request/error counts and explain the decision."""

        event_timestamp = _finite_real("timestamp", timestamp)
        request_count = _integer("requests", requests, minimum=0)
        error_count = _integer("errors", errors, minimum=0)
        if error_count > request_count:
            raise ValueError("errors cannot exceed requests")

        if self._last_timestamp is not None and event_timestamp < self._last_timestamp:
            if self.late_timestamp_policy == "reject":
                raise ValueError(
                    "timestamp must be greater than or equal to the last "
                    "accepted timestamp"
                )
            return self._result(
                input_timestamp=event_timestamp,
                evaluation_timestamp=self._last_timestamp,
                sample_accepted=False,
                late_timestamp=True,
            )

        sample = _TrafficSample(event_timestamp, request_count, error_count)
        self._short.add(sample)
        self._long.add(sample)
        self._last_timestamp = event_timestamp
        return self._result(
            input_timestamp=event_timestamp,
            evaluation_timestamp=event_timestamp,
            sample_accepted=True,
            late_timestamp=False,
        )

    def observe(self, timestamp: float, requests: int, errors: int) -> BurnRateResult:
        """Alias for :meth:`update`."""

        return self.update(timestamp, requests, errors)

    def _window_result(
        self,
        window: _WindowAccumulator,
        *,
        timestamp: float,
        threshold: float,
    ) -> BurnRateWindowResult:
        if window.requests:
            error_rate = window.errors / window.requests
            burn_rate = error_rate / self._error_budget
        else:
            error_rate = 0.0
            burn_rate = 0.0

        enough_traffic = window.requests >= self.minimum_traffic
        data_complete = window.complete_at(timestamp)
        threshold_exceeded = burn_rate >= threshold
        alert = enough_traffic and data_complete and threshold_exceeded
        return BurnRateWindowResult(
            duration=window.duration,
            requests=window.requests,
            errors=window.errors,
            observed_error_rate=error_rate,
            burn_rate=burn_rate,
            threshold=threshold,
            minimum_traffic=self.minimum_traffic,
            enough_traffic=enough_traffic,
            data_complete=data_complete,
            threshold_exceeded=threshold_exceeded,
            alert=alert,
            retained_samples=len(window.samples),
            capacity_evictions=window.capacity_evictions,
        )

    def _result(
        self,
        *,
        input_timestamp: float,
        evaluation_timestamp: float,
        sample_accepted: bool,
        late_timestamp: bool,
    ) -> BurnRateResult:
        short = self._window_result(
            self._short,
            timestamp=evaluation_timestamp,
            threshold=self.fast_burn_threshold,
        )
        long = self._window_result(
            self._long,
            timestamp=evaluation_timestamp,
            threshold=self.slow_burn_threshold,
        )
        fast_alert = short.alert
        slow_alert = long.alert
        if fast_alert and slow_alert:
            alert_kind: AlertKind = "fast_and_slow"
        elif fast_alert:
            alert_kind = "fast"
        elif slow_alert:
            alert_kind = "slow"
        else:
            alert_kind = "none"

        return BurnRateResult(
            input_timestamp=input_timestamp,
            evaluation_timestamp=evaluation_timestamp,
            sample_accepted=sample_accepted,
            late_timestamp=late_timestamp,
            slo_target=self.slo_target,
            error_budget=self._error_budget,
            short=short,
            long=long,
            fast_alert=fast_alert,
            slow_alert=slow_alert,
            alert=fast_alert or slow_alert,
            alert_kind=alert_kind,
        )


__all__ = [
    "AlertKind",
    "BurnRateResult",
    "BurnRateWindowResult",
    "LateTimestampPolicy",
    "MultiWindowBurnRateDetector",
    "PageHinkleyDetector",
    "PageHinkleyResult",
]
