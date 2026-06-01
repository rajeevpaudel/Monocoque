from unittest.mock import MagicMock, patch
import pytest
from ingestion.shared import ingestion_log as il


def _mock_ch(query_result=None):
    mock = MagicMock()
    mock.query_one.return_value = query_result
    return mock


def test_status_ok_when_rows_meet_threshold():
    assert il._status("raw_openf1.laps", 100, "") == "ok"


def test_status_incomplete_when_rows_below_threshold():
    assert il._status("raw_openf1.laps", 10, "") == "incomplete"


def test_status_failed_when_error_msg_present():
    assert il._status("raw_openf1.laps", 0, "timeout") == "failed"


def test_status_empty_for_optional_table_with_zero_rows():
    assert il._status("raw_openf1.pit", 0, "") == "empty"


def test_status_incomplete_for_required_table_with_zero_rows():
    assert il._status("raw_openf1.laps", 0, "") == "incomplete"


def test_needs_ingestion_true_when_no_log_entry():
    with patch("ingestion.shared.ingestion_log.ch", _mock_ch(query_result=None)):
        assert il.needs_ingestion("openf1", "9900", "raw_openf1.laps") is True


def test_needs_ingestion_false_when_status_ok():
    with patch("ingestion.shared.ingestion_log.ch", _mock_ch(query_result="ok")):
        assert il.needs_ingestion("openf1", "9900", "raw_openf1.laps") is False


def test_needs_ingestion_false_when_status_empty():
    with patch("ingestion.shared.ingestion_log.ch", _mock_ch(query_result="empty")):
        assert il.needs_ingestion("openf1", "9900", "raw_openf1.pit") is False


def test_needs_ingestion_true_when_status_incomplete():
    with patch("ingestion.shared.ingestion_log.ch", _mock_ch(query_result="incomplete")):
        assert il.needs_ingestion("openf1", "9900", "raw_openf1.laps") is True


def test_needs_ingestion_true_when_status_failed():
    with patch("ingestion.shared.ingestion_log.ch", _mock_ch(query_result="failed")):
        assert il.needs_ingestion("openf1", "9900", "raw_openf1.laps") is True


def test_record_inserts_ok_status():
    mock_ch = _mock_ch()
    with patch("ingestion.shared.ingestion_log.ch", mock_ch):
        il.record("openf1", "9900", "raw_openf1.laps", 500)
    mock_ch.insert_rows.assert_called_once()
    inserted = mock_ch.insert_rows.call_args[0][1][0]
    assert inserted["status"] == "ok"
    assert inserted["row_count"] == 500
    assert inserted["source"] == "openf1"
    assert inserted["entity_key"] == "9900"
    assert inserted["table_name"] == "raw_openf1.laps"


def test_record_inserts_incomplete_status_for_low_row_count():
    mock_ch = _mock_ch()
    with patch("ingestion.shared.ingestion_log.ch", mock_ch):
        il.record("openf1", "9900", "raw_openf1.laps", 1)
    inserted = mock_ch.insert_rows.call_args[0][1][0]
    assert inserted["status"] == "incomplete"


def test_record_inserts_failed_status_when_error_given():
    mock_ch = _mock_ch()
    with patch("ingestion.shared.ingestion_log.ch", mock_ch):
        il.record("openf1", "9900", "raw_openf1.laps", 0, "connection refused")
    inserted = mock_ch.insert_rows.call_args[0][1][0]
    assert inserted["status"] == "failed"
    assert inserted["error_msg"] == "connection refused"


def test_jolpica_entity_key_format():
    mock_ch = _mock_ch()
    with patch("ingestion.shared.ingestion_log.ch", mock_ch):
        il.record("jolpica", "2025-4", "raw_jolpica.qualifying", 20)
    inserted = mock_ch.insert_rows.call_args[0][1][0]
    assert inserted["entity_key"] == "2025-4"
    assert inserted["source"] == "jolpica"
