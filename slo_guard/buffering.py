"""Bounded event-time reordering and duplicate suppression."""

from __future__ import annotations

import heapq
from collections import OrderedDict
from dataclasses import dataclass
from math import isfinite
from numbers import Real

from .models import TelemetryEvent


@dataclass(frozen=True, slots=True)
class BufferOutcome:
    accepted: bool
    emitted: tuple[TelemetryEvent, ...]
    dropped_reason: str | None = None


class EventTimeBuffer:
    """Reorder bounded-lateness events before stateful analysis.

    The watermark is ``max_seen_timestamp - allowed_lateness``. Records older
    than the current watermark are rejected because accepting them would mutate
    time-window state after its ordering guarantee had already been published.
    """

    def __init__(
        self,
        *,
        allowed_lateness: float = 5.0,
        max_buffer_events: int = 10_000,
        dedupe_capacity: int = 100_000,
    ) -> None:
        if (
            isinstance(allowed_lateness, bool)
            or not isinstance(allowed_lateness, Real)
            or not isfinite(allowed_lateness)
            or allowed_lateness < 0
        ):
            raise ValueError("allowed_lateness must be a non-negative finite number")
        for name, value in (
            ("max_buffer_events", max_buffer_events),
            ("dedupe_capacity", dedupe_capacity),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

        self.allowed_lateness = float(allowed_lateness)
        self.max_buffer_events = max_buffer_events
        self.dedupe_capacity = dedupe_capacity
        self._heap: list[tuple[float, int, TelemetryEvent]] = []
        self._seen_ids: OrderedDict[str, None] = OrderedDict()
        self._sequence = 0
        self._max_seen_timestamp: float | None = None
        self.accepted = 0
        self.dropped_duplicate = 0
        self.dropped_late = 0
        self.dropped_capacity = 0

    @property
    def watermark(self) -> float | None:
        if self._max_seen_timestamp is None:
            return None
        return self._max_seen_timestamp - self.allowed_lateness

    @property
    def buffered(self) -> int:
        return len(self._heap)

    def add(self, event: TelemetryEvent) -> BufferOutcome:
        if event.event_id in self._seen_ids:
            self.dropped_duplicate += 1
            return BufferOutcome(False, (), "duplicate_event")

        current_watermark = self.watermark
        if current_watermark is not None and event.timestamp < current_watermark:
            self.dropped_late += 1
            return BufferOutcome(False, (), "beyond_watermark")

        if (
            self._max_seen_timestamp is None
            or event.timestamp > self._max_seen_timestamp
        ):
            self._max_seen_timestamp = event.timestamp

        emitted = self._pop_ready()
        if len(self._heap) >= self.max_buffer_events:
            self.dropped_capacity += 1
            return BufferOutcome(False, tuple(emitted), "buffer_capacity_exceeded")

        self._remember(event.event_id)
        heapq.heappush(self._heap, (event.timestamp, self._sequence, event))
        self._sequence += 1
        self.accepted += 1
        emitted.extend(self._pop_ready())
        return BufferOutcome(True, tuple(emitted))

    def flush(self) -> tuple[TelemetryEvent, ...]:
        emitted: list[TelemetryEvent] = []
        while self._heap:
            _, _, event = heapq.heappop(self._heap)
            emitted.append(event)
        return tuple(emitted)

    def metrics(self) -> dict[str, int | float | None]:
        return {
            "accepted": self.accepted,
            "buffered": self.buffered,
            "dropped_duplicate": self.dropped_duplicate,
            "dropped_late": self.dropped_late,
            "dropped_capacity": self.dropped_capacity,
            "watermark": self.watermark,
        }

    def _pop_ready(self) -> list[TelemetryEvent]:
        watermark = self.watermark
        if watermark is None:
            return []
        emitted: list[TelemetryEvent] = []
        while self._heap and self._heap[0][0] <= watermark:
            _, _, event = heapq.heappop(self._heap)
            emitted.append(event)
        return emitted

    def _remember(self, event_id: str) -> None:
        self._seen_ids[event_id] = None
        self._seen_ids.move_to_end(event_id)
        while len(self._seen_ids) > self.dedupe_capacity:
            self._seen_ids.popitem(last=False)
