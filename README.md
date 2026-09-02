# Streaming SLO Guard

[![CI](https://github.com/brohum10/streaming-slo-guard/actions/workflows/ci.yml/badge.svg)](https://github.com/brohum10/streaming-slo-guard/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

Streaming SLO Guard is a dependency-free Python monitor that turns request-level
telemetry into explainable reliability signals as the data arrives. It handles
out-of-order events, estimates latency percentiles without retaining every
observation, identifies dominant failure signatures, detects latency shifts,
and measures error-budget burn across two rolling windows.

This is a focused systems project: the interesting work is in the event-time
semantics, probabilistic data structures, safety limits, and evidence attached
to every alert—not in a framework or hosted dashboard.

## Why this project is interesting

- Implements Greenwald–Khanna quantiles, Count-Min Sketch, Space-Saving
  candidates, Page-Hinkley change detection, and SLO burn-rate windows from
  first principles.
- Produces deterministic results by using event-time ordering and seeded
  BLAKE2b hashing instead of Python's process-randomized string hash.
- Places explicit limits on service cardinality, reorder-buffer size,
  deduplication state, window samples, error candidates, and retained alerts.
- Makes approximate answers visible: reports include rank-error and frequency
  bounds rather than presenting estimates as exact values.
- Ships as both an importable library and an NDJSON command-line tool, with no
  runtime packages beyond the Python standard library.

## Architecture at a glance

```text
NDJSON request event
        │
        ▼
 schema validation ───── invalid ─────► bounded error sample
        │
        ▼
 deduplication + event-time heap ─────► duplicate / late / capacity counters
        │                      watermark releases ordered events
        ▼
 cardinality-controlled per-service state
        ├── Greenwald–Khanna sketch ──► p50 / p95 / p99 latency
        ├── Count-Min + Space-Saving ─► frequent error signatures
        ├── Page-Hinkley detector ────► upward latency-change alerts
        └── short + long windows ─────► error-budget burn alerts
                                              │
                                              ▼
                                    cooldown + evidence
                                              │
                                              ▼
                                        JSON report
```

The watermark is `largest observed timestamp - allowed lateness`. Events at or
behind it are released in timestamp order; events that arrive older than an
already-published watermark are rejected instead of rewriting past decisions.
See [the architecture notes](docs/architecture.md) for the full data flow and
trust boundaries.

## Algorithm choices and tradeoffs

| Component | Choice | What it buys | Tradeoff |
| --- | --- | --- | --- |
| Latency percentiles | Greenwald–Khanna summary | Deterministic quantiles with a configurable rank-error bound | Summary grows sublinearly with the stream and is not directly mergeable |
| Error frequencies | Count-Min Sketch | Fixed counter table and no undercount for observed keys | Hash collisions can overestimate counts |
| Top errors | Space-Saving candidates | Enumerates likely heavy hitters with only `k` retained keys | Non-candidates are queryable by Count-Min but cannot be listed |
| Latency drift | Page-Hinkley | Constant-state detection of sustained upward mean changes | Thresholds need workload-specific tuning; detection does not explain root cause |
| Reliability policy | Independent short/long burn-rate windows | Surfaces both rapid and sustained error-budget consumption | Defaults are demonstration settings, not a universal paging policy |
| Event time | Heap + watermark | Deterministic windows under bounded disorder | Events beyond the lateness allowance are counted and dropped |

HTTP 5xx responses consume the error budget. HTTP 4xx responses remain visible
in the input but do not count as service errors by default.

## Quick start

Python 3.11 or newer is required.

```bash
git clone https://github.com/brohum10/streaming-slo-guard.git
cd streaming-slo-guard
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
slo-guard analyze examples/incident.ndjson --pretty
```

On Windows PowerShell, activate the environment with
`.venv\Scripts\Activate.ps1` instead.

## Input format

The analyzer accepts newline-delimited JSON (NDJSON): one JSON object per line.

```json
{"event_id":"req-005","timestamp":1700000004,"service":"checkout","latency_ms":410,"status_code":503,"error_signature":"payment-gateway-timeout"}
```

| Field | Required | Contract |
| --- | --- | --- |
| `event_id` | Yes | Non-empty string, at most 128 characters; used for bounded deduplication |
| `timestamp` | Yes | Non-negative, finite Unix timestamp in seconds |
| `service` | Yes | Non-empty string, at most 128 characters |
| `latency_ms` | Yes | Finite number from 0 through 3,600,000 |
| `status_code` | Yes | Integer from 100 through 599 |
| `error_signature` | No | Non-empty string, at most 256 characters when present |

Malformed records are counted and skipped by default. `--strict` stops at the
first invalid record. Duplicate IDs, events beyond the watermark, buffer
overflow, and service-cardinality overflow have separate rejection counters.

## Analyze a stream

Analyze the included out-of-order incident fixture:

```bash
slo-guard analyze examples/incident.ndjson --pretty
```

Read from standard input by omitting the path or using `-`:

```bash
slo-guard analyze - --pretty
```

Useful policy controls:

```bash
slo-guard analyze telemetry.ndjson \
  --slo-target 0.999 \
  --allowed-lateness 5 \
  --minimum-traffic 25 \
  --max-services 256 \
  --fail-on-alert
```

`--fail-on-alert` returns exit status `2` when any alert was emitted, which
makes the CLI usable as a CI or batch-processing guard. File/input errors return
`1`; a successful analysis without that condition returns `0`.

### Report shape

The following is a schematic shape, not a captured benchmark and not fabricated
measurements. Angle-bracketed entries describe the value type.

```json
{
  "schema_version": "1.0",
  "input": {
    "lines": "<integer>",
    "invalid_records": "<integer>",
    "rejected_records": "<integer>",
    "reported_errors": "<up to 10 line-level explanations>"
  },
  "ingestion": {
    "accepted": "<integer>",
    "buffered": "<integer>",
    "dropped_duplicate": "<integer>",
    "dropped_late": "<integer>",
    "dropped_capacity": "<integer>",
    "watermark": "<Unix seconds or null>"
  },
  "totals": {
    "processed_events": "<integer>",
    "processed_errors": "<integer>",
    "tracked_services": "<integer>",
    "emitted_alerts": "<integer>"
  },
  "services": {
    "<service name>": {
      "error_rate": "<number>",
      "latency_ms": {
        "p50": "<number>",
        "p95": "<number>",
        "p99": "<number>",
        "rank_error_bound": "<integer>"
      },
      "error_signatures": "<ranked estimates with bounds>",
      "burn_rate": "<short/long window evidence>",
      "latency_change": "<detector evidence>"
    }
  },
  "alerts": "<bounded list of evidence-bearing alerts>"
}
```

## Reproducible synthetic benchmark

The benchmark generates a deterministic three-service workload with a checkout
latency shift and error burst. It cross-checks the sketch's p95 against an exact
reference list, checks the expected dominant error signature, records detection
delay, and measures local throughput and traced peak memory.

```bash
slo-guard benchmark --events 50000 --seed 7 --pretty
```

Save the same report while still printing it:

```bash
slo-guard benchmark --events 50000 --seed 7 \
  --output benchmarks/latest.json --pretty
```

The workload and all reported incident data are synthetic and generated
locally. They contain no employer, customer, or production data. Throughput and
memory results are machine- and Python-build-specific; run the command on your
own system rather than treating one checked-in run as a universal performance
claim. The exact latency list exists only inside the benchmark as an accuracy
reference—the guard itself does not retain raw latency history.

### Latest checked-in local run

[`benchmarks/latest.json`](benchmarks/latest.json) records a 50,000-event run
on CPython 3.14 for reproducibility. That run processed 18,711 events/second,
used 7.650 MiB of traced peak memory (including the benchmark's exact reference
list), and returned a p95 within 0.738 ms of the exact result. Its observed rank
error was 111 against a configured bound of 167. The injected latency shift was
detected three synthetic events after it began, with no pre-shift latency
alerts, and the dominant error signature was identified correctly. These are
local measurements, not universal performance claims; rerun the command to
compare your environment.

## Tests and quality checks

Install the development tools, then run the same checks used by CI:

```bash
python -m pip install -e '.[dev]'
python -m pytest
ruff check .
python -m build
```

The test suite covers validation, watermark ordering, deduplication, capacity
limits, sketch error guarantees, deterministic hashing, change detection,
burn-rate windows, cooldowns, snapshots, and end-to-end engine behavior.

## Complexity and state bounds

Let `s` be the current GK summary size, `b` the reorder-buffer cap, `d × w` the
Count-Min table, `k` the candidate count, and `m` the per-window sample cap.

| Operation | Time | Persistent state |
| --- | ---: | ---: |
| Reorder-buffer insert/release | `O(log b)` per heap operation | `O(b)` globally |
| GK insert | `O(s)` in this list-based implementation | `O((1/epsilon) log(epsilon n))` tuples in the standard bound |
| GK quantile query | `O(s)` | no additional persistent state |
| Count-Min update/query | `O(d)` | exactly `O(d × w)` counters per service |
| Heavy-hitter update | `O(d + k)` | `O(d × w + k)` per service |
| Page-Hinkley update | `O(1)` | `O(1)` per service |
| Burn-window update | amortized `O(1)` | at most `O(m)` samples per window and service |

The service set, pending events, dedupe IDs, window samples, heavy-hitter
candidates, Count-Min tables, and retained alert history all have explicit
configuration limits. The GK summary deliberately has a sublinear,
epsilon-dependent size rather than a hard tuple cap; this preserves its rank
guarantee while still avoiding raw-event retention.

## Limitations and production extensions

This repository is a single-process reference implementation, not a hosted
observability service. In particular:

- detector and burn-rate defaults must be calibrated to a service's baseline,
  traffic shape, and actual SLO policy;
- Page-Hinkley detects an upward mean shift but does not perform root-cause
  analysis;
- state is in memory and is not checkpointed across restarts;
- the original GK algorithm is intentionally not exposed as mergeable, so
  distributed aggregation needs a mergeable sketch or a coordinated reduction;
- event IDs are deduplicated within a bounded recent-ID set, not with durable
  exactly-once guarantees;
- there is no authentication, ingestion server, dashboard, or paging-provider
  integration.

A production evolution would add durable checkpoints, partition ownership,
backpressure, schema/version management, metrics export, policy-as-code, and
alert routing. Cross-partition watermarks and delivery semantics would need to
be designed explicitly rather than inferred from this single-process model.

## License

[MIT](LICENSE)
