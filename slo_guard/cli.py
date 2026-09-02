"""Command-line interface for NDJSON telemetry analysis."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from . import __version__
from .config import GuardConfig
from .engine import StreamingSLOGuard
from .models import TelemetryEvent, TelemetryValidationError

MAX_REPORTED_INPUT_ERRORS = 10


@dataclass(frozen=True, slots=True)
class InputRecordError(ValueError):
    """A line-level input failure used by strict mode."""

    line_number: int
    reason: str

    def __str__(self) -> str:
        return f"line {self.line_number}: {self.reason}"


def analyze_stream(
    stream: IO[str],
    *,
    config: GuardConfig | None = None,
    strict: bool = False,
) -> dict[str, object]:
    """Analyze newline-delimited JSON from a text stream."""

    guard = StreamingSLOGuard(config)
    lines = 0
    blank_lines = 0
    invalid_records = 0
    rejected_records = 0
    errors: list[dict[str, object]] = []

    for line_number, raw_line in enumerate(stream, start=1):
        lines += 1
        line = raw_line.strip()
        if not line:
            blank_lines += 1
            continue
        try:
            payload = json.loads(line)
            event = TelemetryEvent.from_mapping(payload)
        except (json.JSONDecodeError, TelemetryValidationError) as exc:
            invalid_records += 1
            if strict:
                raise InputRecordError(line_number, str(exc)) from exc
            if len(errors) < MAX_REPORTED_INPUT_ERRORS:
                errors.append({"line": line_number, "reason": str(exc)})
            continue

        outcome = guard.ingest(event)
        if not outcome.accepted:
            rejected_records += 1

    guard.flush()
    report = guard.snapshot()
    report["input"] = {
        "lines": lines,
        "blank_lines": blank_lines,
        "invalid_records": invalid_records,
        "rejected_records": rejected_records,
        "reported_errors": errors,
        "errors_truncated": invalid_records > len(errors),
    }
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="slo-guard",
        description="Analyze request telemetry with memory-efficient algorithms.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze = subparsers.add_parser(
        "analyze", help="analyze an NDJSON file or standard input"
    )
    analyze.add_argument(
        "source",
        nargs="?",
        default="-",
        help="NDJSON path; use - or omit it for standard input",
    )
    analyze.add_argument("--pretty", action="store_true", help="indent JSON output")
    analyze.add_argument(
        "--strict",
        action="store_true",
        help="stop at the first malformed or invalid input record",
    )
    analyze.add_argument(
        "--fail-on-alert",
        action="store_true",
        help="exit with status 2 when the report contains an alert",
    )
    analyze.add_argument("--slo-target", type=float, default=0.999)
    analyze.add_argument("--allowed-lateness", type=float, default=5.0)
    analyze.add_argument("--minimum-traffic", type=int, default=25)
    analyze.add_argument("--max-services", type=int, default=256)

    benchmark = subparsers.add_parser(
        "benchmark", help="run the deterministic synthetic workload"
    )
    benchmark.add_argument("--events", type=int, default=50_000)
    benchmark.add_argument("--seed", type=int, default=7)
    benchmark.add_argument("--output", type=Path)
    benchmark.add_argument("--pretty", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "benchmark":
        return _benchmark_command(args, parser)
    return _analyze_command(args, parser)


def _analyze_command(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    try:
        config = GuardConfig(
            slo_target=args.slo_target,
            allowed_lateness_seconds=args.allowed_lateness,
            minimum_window_traffic=args.minimum_traffic,
            max_services=args.max_services,
        )
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))

    try:
        with (
            nullcontext(sys.stdin)
            if args.source == "-"
            else Path(args.source).open(encoding="utf-8")
        ) as stream:
            report = analyze_stream(stream, config=config, strict=args.strict)
    except (OSError, UnicodeError, InputRecordError) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1

    _print_json(report, pretty=args.pretty)
    alerts = report["totals"]["emitted_alerts"]  # type: ignore[index]
    return 2 if args.fail_on_alert and alerts else 0


def _benchmark_command(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> int:
    from .benchmark import run_benchmark

    try:
        result = run_benchmark(event_count=args.events, seed=args.seed)
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))

    serialized = json.dumps(
        result,
        indent=2 if args.pretty else None,
        sort_keys=True,
        allow_nan=False,
    )
    if args.output is not None:
        try:
            args.output.write_text(serialized + "\n", encoding="utf-8")
        except OSError as exc:
            print(json.dumps({"error": str(exc)}), file=sys.stderr)
            return 1
    print(serialized)
    return 0


def _print_json(payload: dict[str, object], *, pretty: bool) -> None:
    print(
        json.dumps(
            payload,
            indent=2 if pretty else None,
            sort_keys=True,
            allow_nan=False,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
