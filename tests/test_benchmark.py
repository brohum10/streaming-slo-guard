from slo_guard.benchmark import run_benchmark


def test_benchmark_cross_checks_algorithms_on_synthetic_incident() -> None:
    result = run_benchmark(event_count=1_500, seed=7)

    assert result["state"]["processed_events"] == 1_500  # type: ignore[index]
    assert (
        result["accuracy"]["observed_rank_error"]
        <= result["accuracy"][  # type: ignore[index]
            "configured_rank_error_bound"
        ]
    )
    incident = result["incident_detection"]  # type: ignore[assignment]
    assert incident["latency_change_alerts"] >= 1  # type: ignore[index]
    assert incident["pre_shift_latency_alerts"] == 0  # type: ignore[index]
    assert incident["first_checkout_change_delay_events"] >= 0  # type: ignore[index]
    assert incident["error_budget_burn_alerts"] >= 1  # type: ignore[index]
    assert incident["top_signature_matched"] is True  # type: ignore[index]
