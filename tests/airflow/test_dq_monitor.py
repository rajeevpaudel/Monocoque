import sys
from unittest.mock import MagicMock

# Stub only the Apache Airflow sub-modules that dq_monitor.py imports at the
# top level. We do NOT stub 'airflow' itself because the local airflow/
# directory is a namespace package that Python needs to traverse in order to
# find airflow/dags/dq_monitor.py.
_airflow_stubs = {
    "airflow.decorators": MagicMock(),
}
for _mod, _mock in _airflow_stubs.items():
    sys.modules.setdefault(_mod, _mock)

from airflow.dags.dq_monitor import parse_run_results  # noqa: E402, I001 — must follow the airflow stub above


def test_parse_run_results_extracts_fail_and_error():
    run_results = {
        "results": [
            {"unique_id": "test.f1_warehouse.assert_match_rate", "status": "fail", "failures": 20, "message": "20 mismatches"},
            {"unique_id": "test.f1_warehouse.assert_weather_coverage", "status": "error", "failures": 0, "message": "compile error"},
            {"unique_id": "test.f1_warehouse.assert_no_future_races", "status": "pass", "failures": 0, "message": ""},
        ]
    }
    failures = parse_run_results(run_results)
    assert len(failures) == 2
    assert failures[0]["test_name"] == "assert_match_rate"
    assert failures[0]["failures"] == 20
    assert failures[1]["test_name"] == "assert_weather_coverage"
    assert failures[1]["status"] == "error"


def test_parse_run_results_ignores_passing_tests():
    run_results = {"results": [{"unique_id": "test.x.assert_ok", "status": "pass"}]}
    assert parse_run_results(run_results) == []


def test_parse_run_results_handles_empty_results():
    assert parse_run_results({"results": []}) == []
    assert parse_run_results({}) == []
