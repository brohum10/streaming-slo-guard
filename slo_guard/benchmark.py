"""Deterministic synthetic benchmark and accuracy cross-check."""

from __future__ import annotations

import bisect
import math
import platform
import random
import sys
import time
import tracemalloc

from .config import GuardConfig
from .engine import StreamingSLOGuard
from .models import TelemetryEvent


def run_benchmark(*, event_count: int = 50_000, seed: int = 7) -> dict[str, object]:
    """Run a repeatable incident workload and return measured results.

    The exact latency list is retained only to check the sketch's answer. It is
    benchmark reference state and is not part of ``StreamingSLOGuard``.
    """

    if isinstance(event_count, bool) or not isinstance(event_count, int):
        raise TypeError("event_count must be an integer")
    if event_count < 1_000:
        raise ValueError("event_count must be at least 1000")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise TypeError("seed must be an integer")

    rng = random.Random(seed)
    config = GuardConfig(
        slo_target=0.99,
        short_window_seconds=30,
        long_window_seconds=120,
        fast_burn_threshold=4,
        slow_burn_threshold=2,
        minimum_window_traffic=30,
        allowed_lateness_seconds=0,
        alert_cooldown_seconds=60,
        drift_warmup=40,
        drift_delta=0.5,
        drift_threshold=600,
        drift_alpha=0.995,
        max_window_samples=10_000,
    )
    guard = StreamingSLOGuard(config)
    services = ("checkout", "catalog", "search")
    base_latency = {"checkout": 75.0, "catalog": 32.0, "search": 48.0}
    shift_start = math.floor(event_count * 0.60)
    burst_start = math.floor(event_count * 0.70)
    burst_end = math.floor(event_count * 0.78)
    checkout_latencies: list[float] = []
    first_change_index: int | None = None
    latency_alerts = 0
    pre_shift_latency_alerts = 0
    burn_alerts = 0

    tracemalloc.start()
    started = time.perf_counter()
    for index in range(event_count):
        service = services[index % len(services)]
        latency = max(0.1, rng.gauss(base_latency[service], 7.5))
        if service == "checkout" and index >= shift_start:
            latency += 140

        in_error_burst = service == "checkout" and burst_start <= index < burst_end
        is_error = rng.random() < (0.30 if in_error_burst else 0.001)
        signature = None
        if is_error:
            selector = rng.random()
            if selector < 0.80:
                signature = "payment-gateway-timeout"
            elif selector < 0.95:
                signature = "database-pool-exhausted"
            else:
                signature = "unknown-upstream"

        if service == "checkout":
            checkout_latencies.append(latency)
        outcome = guard.ingest(
            TelemetryEvent(
                event_id=f"bench-{index}",
                timestamp=index * 0.05,
                service=service,
                latency_ms=latency,
                status_code=503 if is_error else 200,
                error_signature=signature,
            )
        )
        for alert in outcome.alerts:
            if alert.alert_type == "latency_change":
                latency_alerts += 1
                if index < shift_start:
                    pre_shift_latency_alerts += 1
                elif first_change_index is None and service == "checkout":
                    first_change_index = index
            elif alert.alert_type == "error_budget_burn":
                burn_alerts += 1

    guard.flush()
    elapsed = time.perf_counter() - started
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    report = guard.snapshot()
    checkout = report["services"]["checkout"]  # type: ignore[index]
    approximate_p95 = checkout["latency_ms"]["p95"]  # type: ignore[index]
    ranked = sorted(checkout_latencies)
    exact_p95 = ranked[math.ceil(0.95 * len(ranked)) - 1]
    target_rank = math.ceil(0.95 * len(ranked))
    lower_rank = bisect.bisect_left(ranked, approximate_p95) + 1
    upper_rank = bisect.bisect_right(ranked, approximate_p95)
    if lower_rank <= target_rank <= upper_rank:
        rank_error = 0
    else:
        rank_error = min(abs(lower_rank - target_rank), abs(upper_rank - target_rank))

    hitters = checkout["error_signatures"]  # type: ignore[index]
    observed_top = hitters[0]["key"] if hitters else None
    delay = None if first_change_index is None else first_change_index - shift_start
    return {
        "benchmark": "synthetic-checkout-incident-v1",
        "event_count": event_count,
        "seed": seed,
        "elapsed_seconds": round(elapsed, 6),
        "throughput_events_per_second": round(event_count / elapsed, 2),
        "peak_traced_memory_mib": round(peak_bytes / (1024 * 1024), 3),
        "accuracy": {
            "checkout_observations": len(ranked),
            "exact_p95_ms": round(exact_p95, 6),
            "sketch_p95_ms": round(approximate_p95, 6),
            "absolute_value_error_ms": round(abs(approximate_p95 - exact_p95), 6),
            "observed_rank_error": rank_error,
            "configured_rank_error_bound": math.ceil(
                config.quantile_epsilon * len(ranked)
            ),
        },
        "incident_detection": {
            "latency_change_alerts": latency_alerts,
            "pre_shift_latency_alerts": pre_shift_latency_alerts,
            "error_budget_burn_alerts": burn_alerts,
            "first_checkout_change_delay_events": delay,
            "expected_top_error_signature": "payment-gateway-timeout",
            "observed_top_error_signature": observed_top,
            "top_signature_matched": observed_top == "payment-gateway-timeout",
        },
        "state": {
            "processed_events": report["totals"]["processed_events"],  # type: ignore[index]
            "tracked_services": report["totals"]["tracked_services"],  # type: ignore[index]
            "checkout_quantile_tuples": checkout["latency_ms"][  # type: ignore[index]
                "summary_tuples"
            ],
            "retained_alerts": report["totals"]["retained_alerts"],  # type: ignore[index]
        },
        "runtime": {
            "python": platform.python_version(),
            "implementation": platform.python_implementation(),
            "platform": sys.platform,
        },
        "notes": [
            "The workload is deterministic and entirely synthetic.",
            "Timing and traced-memory values vary by machine and Python build.",
            "Exact latency storage exists only as the benchmark accuracy reference.",
        ],
    }


__all__ = ["run_benchmark"]
