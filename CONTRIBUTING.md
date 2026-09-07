# Contributing to Streaming SLO Guard

Contributions are welcome when they preserve bounded memory, deterministic behavior, and explainable alert evidence.

## Local checks

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
ruff format --check .
ruff check .
pytest --cov=slo_guard --cov-report=term-missing --cov-fail-under=90
python -m build
```

## Expectations

- State the error or approximation bound affected by an algorithmic change.
- Preserve deterministic hashing and event-time behavior.
- Add adversarial tests for disorder, duplication, cardinality, or memory limits.
- Keep alert evidence and approximation uncertainty visible to operators.
- Update the benchmark record for changes intended to improve throughput or memory use.

Pull requests should include algorithmic complexity, bounds, tests performed, and benchmark evidence when relevant.
