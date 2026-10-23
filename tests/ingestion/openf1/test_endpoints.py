from unittest.mock import patch

from ingestion.openf1 import endpoints


def test_get_position_parses_records_and_drops_missing_positions():
    records = [
        {"date": "2024-03-02T15:03:00+00:00", "session_key": 9472, "meeting_key": 1229,
         "driver_number": 1, "position": 1},
        {"date": "2024-03-02T15:04:00+00:00", "session_key": 9472, "meeting_key": 1229,
         "driver_number": 16, "position": None},
    ]
    with patch("ingestion.openf1.endpoints.get", return_value=records) as mock_get:
        rows = endpoints.get_position(9472)
    mock_get.assert_called_once_with("position", {"session_key": 9472})
    assert len(rows) == 1
    assert rows[0].driver_number == 1
    assert rows[0].position == 1
    assert rows[0].meeting_key == 1229
    assert '"position": 1' in rows[0].raw_json or '"position":1' in rows[0].raw_json
