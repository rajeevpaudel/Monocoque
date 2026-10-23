import sys
from unittest.mock import patch

from ingestion.openf1 import incremental


def test_main_parses_args_and_delegates_to_ingest_session():
    argv = ["prog", "--session-key", "9158"]
    with patch.object(sys, "argv", argv), \
         patch("ingestion.openf1.incremental.ingest_session") as mock_ingest:
        incremental.main()
    mock_ingest.assert_called_once_with(9158, skip_telemetry=False)


def test_main_passes_skip_telemetry_flag():
    argv = ["prog", "--session-key", "9158", "--skip-telemetry"]
    with patch.object(sys, "argv", argv), \
         patch("ingestion.openf1.incremental.ingest_session") as mock_ingest:
        incremental.main()
    mock_ingest.assert_called_once_with(9158, skip_telemetry=True)
