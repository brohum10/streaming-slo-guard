import io
import json

import pytest

from slo_guard.cli import InputRecordError, analyze_stream, main


def line(
    event_id: str,
    timestamp: float,
    *,
    status: int = 200,
    signature: str | None = None,
) -> str:
    payload = {
        "event_id": event_id,
        "timestamp": timestamp,
        "service": "checkout",
        "latency_ms": 25,
        "status_code": status,
    }
    if signature is not None:
        payload["error_signature"] = signature
    return json.dumps(payload)


def test_analyze_stream_skips_invalid_records_and_reports_them() -> None:
    source = io.StringIO("\n".join([line("a", 1), "not-json", line("b", 2)]))

    report = analyze_stream(source)

    assert report["totals"]["processed_events"] == 2  # type: ignore[index]
    assert report["input"]["invalid_records"] == 1  # type: ignore[index]
    assert report["input"]["reported_errors"][0]["line"] == 2  # type: ignore[index]


def test_analyze_stream_strict_mode_stops_on_bad_line() -> None:
    with pytest.raises(InputRecordError, match="line 2"):
        analyze_stream(io.StringIO(f"{line('a', 1)}\n{{bad\n"), strict=True)


def test_main_analyzes_a_file_and_prints_json(tmp_path, capsys) -> None:
    source = tmp_path / "events.ndjson"
    source.write_text(line("a", 1) + "\n", encoding="utf-8")

    exit_code = main(["analyze", str(source), "--pretty"])

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["totals"]["processed_events"] == 1


def test_main_fail_on_alert_uses_distinct_exit_status(tmp_path, capsys) -> None:
    source = tmp_path / "incident.ndjson"
    source.write_text(
        "\n".join(
            [
                line("a", 1),
                line("b", 2, status=503, signature="upstream-timeout"),
            ]
        ),
        encoding="utf-8",
    )

    exit_code = main(
        [
            "analyze",
            str(source),
            "--minimum-traffic",
            "2",
            "--fail-on-alert",
        ]
    )

    assert exit_code == 2
    assert json.loads(capsys.readouterr().out)["totals"]["emitted_alerts"] >= 1


def test_main_returns_error_for_missing_file(capsys) -> None:
    exit_code = main(["analyze", "/definitely/missing/events.ndjson"])

    assert exit_code == 1
    assert "error" in json.loads(capsys.readouterr().err)
