import sys
from unittest.mock import MagicMock

# Stub only the Apache Airflow sub-modules that dq_monitor.py imports at the
# top level.  We do NOT stub 'airflow' itself because the local airflow/
# directory is a namespace package that Python needs to traverse in order to
# find airflow/dags/dq_monitor.py.
_airflow_stubs = {
    "airflow.decorators": MagicMock(),
    "airflow.operators": MagicMock(),
    "airflow.operators.trigger_dagrun": MagicMock(),
}
for _mod, _mock in _airflow_stubs.items():
    sys.modules.setdefault(_mod, _mock)

sys.path.insert(0, '/home/rajeev/workspace/f1')

from airflow.dags.dq_monitor import classify_failures


def test_bulk_mismatch_is_fixable():
    failures = [
        {
            "test_name": "assert_match_rate",
            "season": 2024,
            "round": 22,
            "session_key": 9640,
            "failures": 20,
        }
    ]
    fixable, investigate = classify_failures(failures)
    assert len(fixable) == 1
    assert fixable[0]["action"] == "reingest_session"
    assert fixable[0]["session_key"] == 9640
    assert len(investigate) == 0


def test_dim_sessions_gap_is_fixable():
    failures = [
        {
            "test_name": "assert_dim_sessions_coverage",
            "season": 2024,
            "round": 22,
            "failures": 1,
        }
    ]
    fixable, investigate = classify_failures(failures)
    assert len(fixable) == 1
    assert fixable[0]["action"] == "reingest_jolpica"
    assert fixable[0]["season"] == 2024


def test_unknown_failure_needs_investigation():
    failures = [
        {"test_name": "assert_weather_coverage", "season": 2024, "round": 5, "failures": 3}
    ]
    fixable, investigate = classify_failures(failures)
    assert len(fixable) == 0
    assert len(investigate) == 1


def test_empty_failures_returns_empty():
    fixable, investigate = classify_failures([])
    assert fixable == []
    assert investigate == []
