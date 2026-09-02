"""Memory-efficient sketches for streaming service-level telemetry.

The implementations in this module use only the Python standard library and do
not rely on Python's process-randomized :func:`hash`.  Given the same inputs and
seed, their results are reproducible across processes.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterator
from dataclasses import dataclass
from numbers import Real


def _finite_float(value: Real, *, name: str) -> float:
    """Return *value* as a finite float, rejecting booleans and non-numbers."""

    if isinstance(value, bool) or not isinstance(value, Real):
        raise TypeError(f"{name} must be a real number")
    try:
        converted = float(value)
    except OverflowError as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(converted):
        raise ValueError(f"{name} must be finite")
    return converted


def _positive_int(value: int, *, name: str) -> int:
    """Validate a strictly positive, non-boolean integer."""

    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _key(value: str) -> str:
    """Validate a Count-Min/Space-Saving key."""

    if not isinstance(value, str):
        raise TypeError("key must be a string")
    return value


@dataclass(slots=True)
class _GKTuple:
    value: float
    gap: int
    delta: int


class GKQuantileSketch:
    """Deterministic Greenwald--Khanna streaming quantile summary.

    For ``0 < q < 1`` over ``N`` observations, :meth:`query` returns an
    observed value with an admissible one-based rank within ``epsilon * N`` of
    ``ceil(q * N)``.  Equal observations can represent any rank in their tied
    interval.  The endpoints are exact: querying zero returns the minimum and
    querying one returns the maximum.

    The summary stores ``O((1 / epsilon) * log(epsilon * N))`` tuples rather
    than retaining the stream.  Insertion is deterministic, including for
    duplicate values.  This class deliberately has no ``merge`` method: the
    original GK summary is not generally mergeable without weakening or
    carefully recomputing its guarantees.

    Args:
        epsilon: Maximum normalized rank error.  It must be finite and satisfy
            ``0 < epsilon < 1``.
    """

    __slots__ = ("_count", "_epsilon", "_summary")

    def __init__(self, epsilon: Real = 0.01) -> None:
        checked = _finite_float(epsilon, name="epsilon")
        if not 0.0 < checked < 1.0:
            raise ValueError("epsilon must satisfy 0 < epsilon < 1")
        self._epsilon = checked
        self._count = 0
        self._summary: list[_GKTuple] = []

    @property
    def epsilon(self) -> float:
        """The configured normalized rank-error bound."""

        return self._epsilon

    @property
    def count(self) -> int:
        """Number of observations inserted into the sketch."""

        return self._count

    @property
    def size(self) -> int:
        """Number of tuples currently retained by the summary."""

        return len(self._summary)

    def insert(self, value: Real) -> None:
        """Add one finite numeric observation to the summary."""

        observation = _finite_float(value, name="value")
        index = self._insertion_index(observation)
        self._count += 1

        if index == 0 or index == len(self._summary):
            delta = 0
        else:
            delta = max(math.floor(2.0 * self._epsilon * self._count) - 1, 0)
        self._summary.insert(index, _GKTuple(observation, 1, delta))
        self._compress()

    def query(self, quantile: Real) -> float:
        """Return an approximate value for a quantile in the closed interval.

        Raises:
            ValueError: If the sketch is empty, the quantile is not finite, or
                the quantile lies outside ``[0, 1]``.
            TypeError: If the quantile is not a real number.
        """

        q = _finite_float(quantile, name="quantile")
        if not 0.0 <= q <= 1.0:
            raise ValueError("quantile must satisfy 0 <= quantile <= 1")
        if not self._summary:
            raise ValueError("cannot query an empty quantile sketch")
        if q == 0.0:
            return self._summary[0].value
        if q == 1.0:
            return self._summary[-1].value

        desired_rank = math.ceil(q * self._count)
        permitted_rank = desired_rank + self._epsilon * self._count
        rank_min = self._summary[0].gap
        previous = self._summary[0].value

        for entry in self._summary[1:]:
            rank_min += entry.gap
            if rank_min + entry.delta > permitted_rank:
                return previous
            previous = entry.value
        return self._summary[-1].value

    def _insertion_index(self, value: float) -> int:
        """Find the stable right-hand insertion point without allocating keys."""

        low = 0
        high = len(self._summary)
        while low < high:
            middle = (low + high) // 2
            if value < self._summary[middle].value:
                high = middle
            else:
                low = middle + 1
        return low

    def _compress(self) -> None:
        """Coalesce adjacent tuples while preserving the GK invariant."""

        threshold = math.floor(2.0 * self._epsilon * self._count)
        # Preserve tuple zero so the observed minimum always remains exact.
        for index in range(len(self._summary) - 2, 0, -1):
            current = self._summary[index]
            successor = self._summary[index + 1]
            if current.gap + successor.gap + successor.delta <= threshold:
                successor.gap += current.gap
                del self._summary[index]


# A concise compatibility name for callers that do not need to select among
# multiple quantile algorithms.
QuantileSketch = GKQuantileSketch


class CountMinSketch:
    """A deterministic Count-Min Sketch for non-negative string frequencies.

    Estimates never undercount an inserted key.  With width ``w`` and depth
    ``d``, the usual Count-Min bounds apply: choosing ``w = ceil(e / error)``
    and ``d = ceil(log(1 / failure_probability))`` bounds additive error by
    ``error * total_count`` with the corresponding probability under the hash
    model.  The allocated counter table always contains exactly ``width *
    depth`` Python integer counters.

    Hashing uses seeded BLAKE2b rather than Python's randomized string hash, so
    results are reproducible across interpreter processes and input order.
    """

    __slots__ = ("_count", "_depth", "_seed_bytes", "_table", "_width")

    _PERSON = b"slo-cms-v1"
    _MAX_SEED = (1 << 64) - 1
    _MAX_DEPTH = (1 << 32) - 1

    def __init__(self, width: int, depth: int, seed: int = 0) -> None:
        self._width = _positive_int(width, name="width")
        self._depth = _positive_int(depth, name="depth")
        if self._depth > self._MAX_DEPTH:
            raise ValueError(f"depth must not exceed {self._MAX_DEPTH}")
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise TypeError("seed must be an integer")
        if not 0 <= seed <= self._MAX_SEED:
            raise ValueError(f"seed must satisfy 0 <= seed <= {self._MAX_SEED}")

        self._seed_bytes = seed.to_bytes(8, "big", signed=False)
        self._table = [[0] * self._width for _ in range(self._depth)]
        self._count = 0

    @property
    def width(self) -> int:
        """Number of counters in each row."""

        return self._width

    @property
    def depth(self) -> int:
        """Number of independently seeded counter rows."""

        return self._depth

    @property
    def count(self) -> int:
        """Total positive weight supplied to :meth:`update`."""

        return self._count

    @property
    def size(self) -> int:
        """The fixed number of allocated counters."""

        return self._width * self._depth

    def update(self, key: str, count: int = 1) -> None:
        """Increment *key* by a positive integer weight."""

        checked_key = _key(key)
        weight = _positive_int(count, name="count")
        encoded = checked_key.encode("utf-8")
        for row, column in enumerate(self._columns(encoded)):
            self._table[row][column] += weight
        self._count += weight

    def estimate(self, key: str) -> int:
        """Return the minimum counter associated with *key*."""

        encoded = _key(key).encode("utf-8")
        return min(
            self._table[row][column]
            for row, column in enumerate(self._columns(encoded))
        )

    def _columns(self, encoded_key: bytes) -> Iterator[int]:
        """Yield one deterministic counter position per row."""

        for row in range(self._depth):
            digest = hashlib.blake2b(
                row.to_bytes(4, "big") + encoded_key,
                digest_size=8,
                key=self._seed_bytes,
                person=self._PERSON,
            ).digest()
            yield int.from_bytes(digest, "big") % self._width


@dataclass(frozen=True, slots=True)
class HeavyHitter:
    """One bounded Space-Saving frequency estimate.

    The true frequency of a retained key is no greater than ``estimate`` and
    no less than ``estimate - error``.
    """

    key: str
    estimate: int
    error: int

    @property
    def lower_bound(self) -> int:
        """Deterministic lower bound implied by Space-Saving."""

        return self.estimate - self.error


@dataclass(slots=True)
class _Candidate:
    estimate: int
    error: int


class HeavyHitters:
    """Bounded frequent-key discovery backed by Count-Min and Space-Saving.

    Count-Min can estimate a supplied key but cannot enumerate keys.  This
    class therefore maintains at most ``k`` Space-Saving candidates alongside
    a fixed Count-Min table.  :meth:`estimate` is available for any key, while
    :meth:`top_k` returns deterministic, ranked candidate estimates with their
    Space-Saving error bounds.  Total memory is ``O(width * depth + k)``.
    """

    __slots__ = ("_candidates", "_capacity", "_sketch")

    def __init__(
        self,
        k: int,
        *,
        width: int = 2048,
        depth: int = 5,
        seed: int = 0,
    ) -> None:
        self._capacity = _positive_int(k, name="k")
        self._sketch = CountMinSketch(width=width, depth=depth, seed=seed)
        self._candidates: dict[str, _Candidate] = {}

    @property
    def capacity(self) -> int:
        """Maximum number of keys retained for enumeration."""

        return self._capacity

    @property
    def size(self) -> int:
        """Current number of retained candidate keys."""

        return len(self._candidates)

    @property
    def count(self) -> int:
        """Total weight observed by the tracker."""

        return self._sketch.count

    def update(self, key: str, count: int = 1) -> None:
        """Observe a positive integer frequency increment for *key*."""

        checked_key = _key(key)
        weight = _positive_int(count, name="count")
        # Validate both arguments before mutating either bounded structure.
        self._sketch.update(checked_key, weight)

        candidate = self._candidates.get(checked_key)
        if candidate is not None:
            candidate.estimate += weight
            return
        if len(self._candidates) < self._capacity:
            self._candidates[checked_key] = _Candidate(weight, 0)
            return

        # Ties are resolved lexicographically to make replacement independent
        # of dictionary iteration details.
        evicted_key, evicted = min(
            self._candidates.items(),
            key=lambda item: (item[1].estimate, item[0]),
        )
        del self._candidates[evicted_key]
        self._candidates[checked_key] = _Candidate(
            estimate=evicted.estimate + weight,
            error=evicted.estimate,
        )

    def estimate(self, key: str) -> int:
        """Return the Count-Min estimate for any supplied key."""

        return self._sketch.estimate(key)

    def top_k(self, limit: int | None = None) -> list[HeavyHitter]:
        """Return retained candidates by descending approximate frequency.

        Args:
            limit: Maximum results to return.  ``None`` uses the configured
                capacity; zero returns an empty list.
        """

        if limit is None:
            result_limit = self._capacity
        else:
            if isinstance(limit, bool) or not isinstance(limit, int):
                raise TypeError("limit must be an integer or None")
            if limit < 0:
                raise ValueError("limit must be non-negative")
            result_limit = limit

        ranked = sorted(
            (
                HeavyHitter(key, candidate.estimate, candidate.error)
                for key, candidate in self._candidates.items()
            ),
            key=lambda item: (-item.estimate, item.key),
        )
        return ranked[:result_limit]


__all__ = [
    "CountMinSketch",
    "GKQuantileSketch",
    "HeavyHitter",
    "HeavyHitters",
    "QuantileSketch",
]
