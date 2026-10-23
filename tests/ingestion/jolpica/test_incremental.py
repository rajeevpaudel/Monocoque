import sys
from unittest.mock import patch

from ingestion.jolpica import incremental


def test_main_parses_args_and_delegates_to_ingest_round():
    argv = ["prog", "--season", "2025", "--round", "8"]
    with patch.object(sys, "argv", argv), \
         patch("ingestion.jolpica.incremental.ingest_round") as mock_ingest:
        incremental.main()
    mock_ingest.assert_called_once_with(2025, 8)
