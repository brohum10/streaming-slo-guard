from __future__ import annotations

import math
import random
from bisect import bisect_left, bisect_right

import pytest

from slo_guard.sketches import (
    CountMinSketch,
    GKQuantileSketch,
    HeavyHitter,
    HeavyHitters,
)


def test_gk_empty_singleton_and_exact_endpoints() -> None:
    sketch = GKQuantileSketch(epsilon=0.01)

    with pytest.raises(ValueError, match="empty"):
        sketch.query(0.5)

    sketch.insert(7)
    assert sketch.count == 1
    assert sketch.size == 1
    assert sketch.query(0) == 7.0
    assert sketch.query(0.5) == 7.0
    assert sketch.query(1) == 7.0

    for value in (12, -4, 9, -1):
        sketch.insert(value)
    assert sketch.query(0) == -4.0
    assert sketch.query(1) == 12.0


def test_gk_deterministic_rank_error_and_bounded_memory() -> None:
    epsilon = 0.01
    values = list(range(10_000))
    random.Random(734_991).shuffle(values)
    sketch = GKQuantileSketch(epsilon=epsilon)
    for value in values:
        sketch.insert(value)

    for quantile in (0.001, 0.01, 0.1, 0.25, 0.5, 0.75, 0.9, 0.99, 0.999):
        result = sketch.query(quantile)
        actual_rank = int(result) + 1
        desired_rank = math.ceil(quantile * len(values))
        assert abs(actual_rank - desired_rank) <= epsilon * len(values)

    assert sketch.count == len(values)
    assert sketch.size < 200
    assert sketch.size < sketch.count


def test_gk_results_are_observed_and_monotonic_with_duplicates() -> None:
    values = [0.0] * 200 + [1.0] * 400 + [2.0] * 300 + [10.0] * 100
    random.Random(19).shuffle(values)
    sketch = GKQuantileSketch(epsilon=0.02)
    for value in values:
        sketch.insert(value)

    quantiles = [index / 100 for index in range(101)]
    results = [sketch.query(quantile) for quantile in quantiles]
    assert results == sorted(results)
    assert set(results) <= set(values)

    ordered = sorted(values)
    for quantile, result in zip(quantiles[1:-1], results[1:-1], strict=True):
        target = math.ceil(quantile * len(ordered))
        lowest_rank = bisect_left(ordered, result) + 1
        highest_rank = bisect_right(ordered, result)
        allowance = sketch.epsilon * sketch.count
        assert lowest_rank <= target + allowance
        assert highest_rank >= target - allowance


def test_gk_internal_rank_summary_invariants() -> None:
    sketch = GKQuantileSketch(epsilon=0.03)
    for value in random.Random(7).sample(range(2_000), 2_000):
        sketch.insert(value)

    entries = sketch._summary
    threshold = math.floor(2 * sketch.epsilon * sketch.count)
    retained_values = [entry.value for entry in entries]
    assert retained_values == sorted(retained_values)
    assert sum(entry.gap for entry in entries) == sketch.count
    assert entries[0].delta == 0
    assert entries[-1].delta == 0
    assert all(entry.gap >= 1 for entry in entries)
    assert all(entry.delta >= 0 for entry in entries)
    assert all(entry.gap + entry.delta <= threshold for entry in entries)


@pytest.mark.parametrize("epsilon", [0, -0.1, 1, 1.1, math.inf, -math.inf, math.nan])
def test_gk_rejects_invalid_epsilon_values(epsilon: float) -> None:
    with pytest.raises(ValueError):
        GKQuantileSketch(epsilon)


def test_gk_accepts_a_valid_large_error_bound() -> None:
    sketch = GKQuantileSketch(0.5)
    for value in range(10):
        sketch.insert(value)

    assert sketch.epsilon == 0.5
    assert sketch.query(0) == 0
    assert sketch.query(1) == 9


@pytest.mark.parametrize("epsilon", [True, "0.01", None])
def test_gk_rejects_non_numeric_epsilon(epsilon: object) -> None:
    with pytest.raises(TypeError):
        GKQuantileSketch(epsilon)  # type: ignore[arg-type]


def test_gk_rejects_invalid_observations_and_quantiles() -> None:
    sketch = GKQuantileSketch()
    for value in (math.inf, -math.inf, math.nan, 10**10_000):
        with pytest.raises(ValueError):
            sketch.insert(value)
    for value in (True, "1", None):
        with pytest.raises(TypeError):
            sketch.insert(value)  # type: ignore[arg-type]
    assert sketch.count == 0

    sketch.insert(1)
    for quantile in (-0.001, 1.001, math.inf, math.nan):
        with pytest.raises(ValueError):
            sketch.query(quantile)
    for quantile in (True, "0.5", None):
        with pytest.raises(TypeError):
            sketch.query(quantile)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("width", "depth", "exception"),
    [
        (0, 3, ValueError),
        (-1, 3, ValueError),
        (8, 0, ValueError),
        (8, -1, ValueError),
        (8, 1 << 32, ValueError),
        (True, 3, TypeError),
        (8, False, TypeError),
        (3.5, 3, TypeError),
        (8, "3", TypeError),
    ],
)
def test_count_min_validates_dimensions(
    width: object, depth: object, exception: type[Exception]
) -> None:
    with pytest.raises(exception):
        CountMinSketch(width=width, depth=depth)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("seed", "exception"),
    [
        (-1, ValueError),
        (1 << 64, ValueError),
        (True, TypeError),
        ("5", TypeError),
    ],
)
def test_count_min_validates_seed(seed: object, exception: type[Exception]) -> None:
    with pytest.raises(exception):
        CountMinSketch(width=8, depth=3, seed=seed)  # type: ignore[arg-type]


def test_count_min_never_underestimates_inserted_keys() -> None:
    sketch = CountMinSketch(width=7, depth=4, seed=2026)
    exact = {"api": 31, "worker": 19, "cache": 11, "db": 7, "queue": 3}
    for key, count in exact.items():
        sketch.update(key, count)

    assert all(sketch.estimate(key) >= count for key, count in exact.items())
    assert sketch.estimate("not-observed") >= 0
    assert sketch.count == sum(exact.values())
    assert sketch.size == sketch.width * sketch.depth == 28


def test_count_min_hashing_is_seeded_deterministic_and_order_independent() -> None:
    updates = [("/v1/search", 5), ("/v1/health", 13), ("café/☕", 8), ("", 2)]
    first = CountMinSketch(width=11, depth=5, seed=99)
    second = CountMinSketch(width=11, depth=5, seed=99)

    for key, count in updates:
        first.update(key, count)
    for key, count in reversed(updates):
        second.update(key, count)

    observed_and_probe_keys = [key for key, _ in updates] + ["other", "雪"]
    assert [first.estimate(key) for key in observed_and_probe_keys] == [
        second.estimate(key) for key in observed_and_probe_keys
    ]


def test_count_min_rejects_invalid_updates_without_mutation() -> None:
    sketch = CountMinSketch(width=8, depth=3)
    for count in (0, -1):
        with pytest.raises(ValueError):
            sketch.update("key", count)
    for count in (True, 1.5, "2"):
        with pytest.raises(TypeError):
            sketch.update("key", count)  # type: ignore[arg-type]
    for key in (1, b"key", None):
        with pytest.raises(TypeError):
            sketch.update(key)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        sketch.estimate(1)  # type: ignore[arg-type]

    assert sketch.count == 0
    assert sketch.estimate("key") == 0


def test_heavy_hitters_returns_ranked_frequencies() -> None:
    tracker = HeavyHitters(k=4, width=256, depth=5, seed=17)
    for key, count in (("checkout", 40), ("search", 25), ("login", 12), ("health", 4)):
        tracker.update(key, count)
    tracker.update("checkout", 2)

    assert tracker.top_k() == [
        HeavyHitter("checkout", 42, 0),
        HeavyHitter("search", 25, 0),
        HeavyHitter("login", 12, 0),
        HeavyHitter("health", 4, 0),
    ]
    assert tracker.top_k(2) == tracker.top_k()[:2]
    assert tracker.top_k(0) == []
    assert tracker.count == 83
    assert tracker.size == tracker.capacity == 4
    assert tracker.estimate("checkout") >= 42


def test_heavy_hitters_space_saving_replacement_is_bounded_and_deterministic() -> None:
    tracker = HeavyHitters(k=2, width=64, depth=4, seed=8)
    tracker.update("alpha", 5)
    tracker.update("beta", 3)
    tracker.update("charlie", 1)

    assert tracker.size == 2
    assert tracker.top_k() == [
        HeavyHitter("alpha", 5, 0),
        HeavyHitter("charlie", 4, 3),
    ]
    replacement = tracker.top_k()[1]
    assert replacement.lower_bound == 1
    assert replacement.lower_bound <= 1 <= replacement.estimate

    # Even a stream of unique keys never grows the candidate dictionary.
    for index in range(1_000):
        tracker.update(f"noise-{index}")
        assert tracker.size <= tracker.capacity

    ranked = tracker.top_k()
    assert [item.estimate for item in ranked] == sorted(
        (item.estimate for item in ranked), reverse=True
    )


def test_heavy_hitters_validates_configuration_updates_and_limit() -> None:
    for invalid_k in (0, -1):
        with pytest.raises(ValueError):
            HeavyHitters(k=invalid_k)
    for invalid_k in (True, 1.5, "2"):
        with pytest.raises(TypeError):
            HeavyHitters(k=invalid_k)  # type: ignore[arg-type]

    tracker = HeavyHitters(k=2)
    with pytest.raises(TypeError):
        tracker.update(1)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        tracker.update("key", 0)
    with pytest.raises(TypeError):
        tracker.top_k(True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        tracker.top_k(-1)

    assert tracker.count == 0
    assert tracker.size == 0
