import pytest

from slo_guard.config import GuardConfig


def test_default_config_is_serializable() -> None:
    config = GuardConfig()

    assert config.slo_target == 0.999
    assert config.to_dict()["max_services"] == 256


@pytest.mark.parametrize(
    "kwargs",
    [
        {"slo_target": 1.0},
        {"short_window_seconds": 0},
        {"short_window_seconds": 20, "long_window_seconds": 10},
        {"quantile_epsilon": 0},
        {"drift_alpha": 1.1},
        {"max_services": 0},
        {"max_window_samples": 5, "minimum_window_traffic": 10},
    ],
)
def test_config_rejects_invalid_limits(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        GuardConfig(**kwargs)  # type: ignore[arg-type]
