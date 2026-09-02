"""Deterministic behavior tests for the streaming reliability detectors."""

from __future__ import annotations

import math
import unittest

from slo_guard.detectors import MultiWindowBurnRateDetector, PageHinkleyDetector


class PageHinkleyDetectorTests(unittest.TestCase):
    def test_stable_series_does_not_report_a_change(self) -> None:
        detector = PageHinkleyDetector(warmup=5, delta=0.01, threshold=2.0, alpha=1.0)

        results = [detector.update(7.5) for _ in range(100)]

        self.assertFalse(any(result.change_detected for result in results))
        self.assertTrue(results[-1].is_warm)
        self.assertEqual(results[-1].count, 100)
        self.assertAlmostEqual(results[-1].mean, 7.5)
        self.assertAlmostEqual(results[-1].deviation, 0.0)
        self.assertEqual(results[-1].cumulative, results[-1].cumulative_sum)

    def test_sustained_upward_mean_shift_is_detected(self) -> None:
        detector = PageHinkleyDetector(warmup=20, delta=0.01, threshold=5.0, alpha=1.0)
        baseline = [detector.update(0.0) for _ in range(40)]
        shifted = [detector.update(1.0) for _ in range(20)]

        self.assertFalse(any(result.detected for result in baseline))
        first_detection = next(result for result in shifted if result.change_detected)
        self.assertGreater(first_detection.deviation, first_detection.threshold)
        self.assertGreater(first_detection.cumulative_sum, -0.4)
        self.assertGreater(first_detection.mean, 0.0)

    def test_alpha_discounts_previous_cumulative_evidence(self) -> None:
        forgotten = PageHinkleyDetector(warmup=1, delta=0.0, threshold=100.0, alpha=0.5)
        retained = PageHinkleyDetector(warmup=1, delta=0.0, threshold=100.0, alpha=1.0)

        for value in (0.0, 2.0, 2.0):
            forgotten_result = forgotten.update(value)
            retained_result = retained.update(value)

        self.assertAlmostEqual(forgotten_result.mean, retained_result.mean)
        self.assertLess(forgotten_result.cumulative_sum, retained_result.cumulative_sum)
        self.assertEqual(forgotten.forgetting_factor, 0.5)

    def test_reset_clears_state_but_preserves_configuration(self) -> None:
        detector = PageHinkleyDetector(warmup=2, delta=0.1, threshold=1.0, alpha=0.9)
        detector.update(1.0)
        detector.update(4.0)

        detector.reset()

        self.assertEqual(detector.count, 0)
        self.assertEqual(detector.mean, 0.0)
        self.assertEqual(detector.cumulative_sum, 0.0)
        self.assertEqual(detector.deviation, 0.0)
        self.assertEqual(detector.alpha, 0.9)
        result = detector.update(3.0)
        self.assertEqual(result.count, 1)
        self.assertFalse(result.is_warm)

    def test_invalid_configuration_and_values_are_rejected(self) -> None:
        invalid_constructors = (
            lambda: PageHinkleyDetector(warmup=0),
            lambda: PageHinkleyDetector(warmup=True),
            lambda: PageHinkleyDetector(delta=-0.1),
            lambda: PageHinkleyDetector(delta=math.inf),
            lambda: PageHinkleyDetector(threshold=0.0),
            lambda: PageHinkleyDetector(alpha=0.0),
            lambda: PageHinkleyDetector(alpha=1.01),
        )
        for constructor in invalid_constructors:
            with (
                self.subTest(constructor=constructor),
                self.assertRaises((TypeError, ValueError)),
            ):
                constructor()

        detector = PageHinkleyDetector()
        for value in (math.nan, math.inf, -math.inf):
            with self.subTest(value=value), self.assertRaises(ValueError):
                detector.update(value)
        with self.assertRaises(TypeError):
            detector.update(True)
        self.assertEqual(detector.count, 0)


def burn_detector(**overrides: object) -> MultiWindowBurnRateDetector:
    options: dict[str, object] = {
        "short_window": 10.0,
        "long_window": 100.0,
        "slo_target": 0.99,
        "fast_burn_threshold": 5.0,
        "slow_burn_threshold": 2.0,
        "minimum_traffic": 100,
        "max_samples": 1_000,
    }
    options.update(overrides)
    return MultiWindowBurnRateDetector(**options)  # type: ignore[arg-type]


class MultiWindowBurnRateDetectorTests(unittest.TestCase):
    def test_healthy_traffic_reports_rates_without_alerting(self) -> None:
        detector = burn_detector()

        result = detector.update(timestamp=0.0, requests=10_000, errors=10)

        self.assertAlmostEqual(result.error_budget, 0.01)
        self.assertAlmostEqual(result.short.observed_error_rate, 0.001)
        self.assertAlmostEqual(result.long_error_rate, 0.001)
        self.assertAlmostEqual(result.short_burn_rate, 0.1)
        self.assertAlmostEqual(result.long_burn_rate, 0.1)
        self.assertEqual(result.fast_burn_threshold, 5.0)
        self.assertEqual(result.slow_burn_threshold, 2.0)
        self.assertFalse(result.fast_alert)
        self.assertFalse(result.slow_alert)
        self.assertFalse(result.should_alert)
        self.assertEqual(result.alert_kind, "none")

    def test_recent_spike_raises_fast_alert_without_slow_alert(self) -> None:
        detector = burn_detector()
        detector.update(timestamp=0.0, requests=10_000, errors=0)

        result = detector.update(timestamp=90.0, requests=100, errors=10)

        self.assertAlmostEqual(result.short_error_rate, 0.1)
        self.assertAlmostEqual(result.short_burn_rate, 10.0)
        self.assertLess(result.long_burn_rate, result.slow_burn_threshold)
        self.assertTrue(result.fast_alert)
        self.assertFalse(result.slow_alert)
        self.assertTrue(result.alert)
        self.assertEqual(result.alert_kind, "fast")

    def test_moderate_long_window_burn_raises_slow_alert(self) -> None:
        detector = burn_detector()

        result = detector.update(timestamp=0.0, requests=1_000, errors=30)

        self.assertAlmostEqual(result.short_burn_rate, 3.0)
        self.assertAlmostEqual(result.long_burn_rate, 3.0)
        self.assertFalse(result.fast_alert)
        self.assertTrue(result.slow_alert)
        self.assertTrue(result.alert)
        self.assertEqual(result.alert_kind, "slow")

    def test_low_traffic_suppresses_alert_but_explains_threshold_crossing(self) -> None:
        detector = burn_detector(minimum_traffic=100)

        result = detector.update(timestamp=0.0, requests=10, errors=10)

        self.assertTrue(result.short.threshold_exceeded)
        self.assertTrue(result.long.threshold_exceeded)
        self.assertFalse(result.short.enough_traffic)
        self.assertFalse(result.long.enough_traffic)
        self.assertFalse(result.alert)

    def test_time_windows_evict_only_expired_samples(self) -> None:
        detector = burn_detector(short_window=5.0, long_window=10.0, minimum_traffic=1)
        detector.update(timestamp=0.0, requests=10, errors=1)

        at_six = detector.update(timestamp=6.0, requests=20, errors=0)
        self.assertEqual(at_six.short.requests, 20)
        self.assertEqual(at_six.long.requests, 30)

        at_eleven = detector.update(timestamp=11.0, requests=30, errors=0)
        self.assertEqual(at_eleven.short.requests, 50)
        self.assertEqual(at_eleven.long.requests, 50)
        self.assertEqual(detector.retained_sample_counts, (2, 2))

    def test_capacity_is_bounded_and_incomplete_windows_do_not_alert(self) -> None:
        detector = burn_detector(
            minimum_traffic=1,
            max_samples=2,
            fast_burn_threshold=1.0,
            slow_burn_threshold=1.0,
        )
        detector.update(timestamp=0.0, requests=10, errors=10)
        detector.update(timestamp=1.0, requests=10, errors=10)

        result = detector.update(timestamp=2.0, requests=10, errors=10)

        self.assertEqual(detector.retained_sample_counts, (2, 2))
        self.assertEqual(result.short.capacity_evictions, 1)
        self.assertEqual(result.long.capacity_evictions, 1)
        self.assertFalse(result.short.data_complete)
        self.assertFalse(result.long.data_complete)
        self.assertTrue(result.short.threshold_exceeded)
        self.assertFalse(result.alert)

    def test_reject_policy_preserves_state_for_late_timestamp(self) -> None:
        detector = burn_detector(late_timestamp_policy="reject")
        accepted = detector.update(timestamp=10.0, requests=100, errors=1)

        with self.assertRaises(ValueError):
            detector.update(timestamp=9.0, requests=900, errors=900)

        self.assertEqual(detector.last_timestamp, 10.0)
        self.assertEqual(detector.retained_sample_counts, (1, 1))
        same_timestamp = detector.update(timestamp=10.0, requests=100, errors=0)
        self.assertEqual(same_timestamp.short.requests, 200)
        self.assertEqual(same_timestamp.long.requests, 200)
        self.assertEqual(accepted.evaluation_timestamp, 10.0)

    def test_ignore_policy_returns_explanation_without_mutating_state(self) -> None:
        detector = burn_detector(late_timestamp_policy="ignore")
        accepted = detector.observe(timestamp=10.0, requests=100, errors=1)

        ignored = detector.observe(timestamp=9.0, requests=900, errors=900)

        self.assertFalse(ignored.sample_accepted)
        self.assertTrue(ignored.late_timestamp)
        self.assertEqual(ignored.input_timestamp, 9.0)
        self.assertEqual(ignored.evaluation_timestamp, 10.0)
        self.assertEqual(ignored.short.requests, accepted.short.requests)
        self.assertEqual(ignored.short.errors, accepted.short.errors)
        self.assertEqual(detector.retained_sample_counts, (1, 1))

    def test_reset_clears_windows_and_ordering_clock(self) -> None:
        detector = burn_detector()
        detector.update(timestamp=10.0, requests=100, errors=1)

        detector.reset()

        self.assertIsNone(detector.last_timestamp)
        self.assertEqual(detector.retained_sample_counts, (0, 0))
        result = detector.update(timestamp=1.0, requests=100, errors=0)
        self.assertTrue(result.accepted)
        self.assertEqual(result.short.requests, 100)

    def test_invalid_configuration_and_observations_are_rejected(self) -> None:
        invalid_constructors = (
            lambda: burn_detector(short_window=0.0),
            lambda: burn_detector(short_window=10.0, long_window=10.0),
            lambda: burn_detector(slo_target=1.0),
            lambda: burn_detector(slo_target=math.nan),
            lambda: burn_detector(fast_burn_threshold=0.0),
            lambda: burn_detector(fast_burn_threshold=1.0, slow_burn_threshold=2.0),
            lambda: burn_detector(minimum_traffic=0),
            lambda: burn_detector(max_samples=0),
            lambda: burn_detector(late_timestamp_policy="buffer"),
        )
        for constructor in invalid_constructors:
            with (
                self.subTest(constructor=constructor),
                self.assertRaises((TypeError, ValueError)),
            ):
                constructor()

        detector = burn_detector()
        invalid_updates = (
            (math.nan, 100, 0),
            (0.0, -1, 0),
            (0.0, 1, -1),
            (0.0, 1, 2),
            (0.0, True, 0),
        )
        for timestamp, requests, errors in invalid_updates:
            with (
                self.subTest(timestamp=timestamp, requests=requests, errors=errors),
                self.assertRaises((TypeError, ValueError)),
            ):
                detector.update(timestamp, requests, errors)
        self.assertIsNone(detector.last_timestamp)


if __name__ == "__main__":
    unittest.main()
