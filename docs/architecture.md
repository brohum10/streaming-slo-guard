# Architecture

Streaming SLO Guard is a single-process reference implementation of an online
reliability monitor. It demonstrates how event-time handling, approximate data
structures, and SLO policy fit together without requiring a metrics database.

```text
NDJSON event
    │
    ▼
schema validation ── invalid ──► bounded error sample
    │
    ▼
ID deduplication ─── duplicate ─► drop counter
    │
    ▼
event-time heap ───── too late ─► drop counter
    │ watermark releases ordered events
    ▼
per-service state (bounded service cardinality)
    ├── GK sketch ──────────────► p50 / p95 / p99 latency
    ├── Count-Min + candidates ─► frequent error signatures
    ├── Page-Hinkley ───────────► latency change signal
    └── time-window counters ───► SLO budget burn signal
                                      │
                                      ▼
                              cooldown + evidence
                                      │
                                      ▼
                                  JSON report
```

## Input and event-time semantics

Each record represents one request and must provide an ID, Unix timestamp,
service, latency, and HTTP status. HTTP 5xx responses consume the error budget;
4xx responses do not by default because they commonly represent caller errors.

The reorder buffer tracks a watermark:

```text
watermark = largest observed timestamp - allowed lateness
```

Events are released in timestamp order when they fall behind the watermark.
Records older than an already-published watermark are rejected. This explicit
policy keeps downstream windows deterministic instead of silently rewriting
past state. A bounded LRU set suppresses retry duplicates.

## Online algorithms

### Greenwald–Khanna quantiles

The GK summary stores tuples rather than every latency. For rank error
`epsilon`, it answers p50/p95/p99 queries with deterministic rank bounds while
compressing observations as the stream grows. This is useful for teaching the
tradeoff between exact storage and approximate answers.

### Count-Min Sketch and bounded candidates

A seeded Count-Min Sketch estimates error-signature frequencies. Multiple hash
rows make collisions unlikely to inflate every counter at once. A bounded
candidate map turns point estimates into a useful top-k view without allowing
unbounded label cardinality.

### Page-Hinkley change detection

Page-Hinkley accumulates deviations from an online mean. Once the deviation
passes a configured threshold after warmup, it emits a latency-change signal.
It detects a change in distribution; it does not explain the root cause.

### Multi-window error-budget burn

For SLO target `S`, the allowed error rate is `1 - S`. A window's burn rate is:

```text
observed error rate / allowed error rate
```

Short and long windows are evaluated independently and include minimum-traffic
gates. The defaults are demonstration settings, not a claim that one policy is
appropriate for every service.

## Complexity and memory bounds

Let `n` be observations for one service, `w × d` the Count-Min table, `k` the
candidate capacity, `b` the reorder-buffer limit, and `m` the capped number of
window samples.

| Operation | Time | Stored state |
| --- | ---: | ---: |
| Reorder insert | `O(log b)` | `O(b)` globally |
| GK insert | amortized summary scan/compression | sublinear in `n`, epsilon-dependent |
| Quantile query | `O(summary size)` | no extra persistent state |
| Count-Min update/query | `O(d)` | `O(w × d)` per service |
| Heavy-hitter candidate update | `O(k + d)` | `O(k + w × d)` per service |
| Page-Hinkley update | `O(1)` | `O(1)` per service |
| Burn-window update | amortized `O(1)` | `O(m)` per service |

Service count, pending events, dedupe IDs, retained alerts, heavy-hitter
candidates, sketch tables, and window samples all have configurable caps.

## Failure and trust boundaries

- Invalid, non-finite, or oversized fields are rejected before state changes.
- Duplicate and late events are counted separately in the final report.
- Maximum service cardinality prevents attacker-controlled service names from
  creating unbounded state.
- Alert history and displayed validation errors are capped.
- Approximate structures expose estimates; the report does not label them as
  exact measurements.

## Production extensions

This repository intentionally stays dependency-free and single-process. A
production design would add durable checkpoints, partition ownership,
backpressure, schema/version management, authentication, metrics export, and
integration with a paging policy. Distributed exactly-once guarantees and
cross-partition watermarks are outside this project's scope.
