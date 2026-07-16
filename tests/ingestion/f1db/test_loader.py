import io
import zipfile
from unittest.mock import patch


def _zip_with(name: str, csv_text: str) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, csv_text)
    buf.seek(0)
    return zipfile.ZipFile(buf)


def test_load_seasons_truncates_before_inserting():
    """Each f1db release is a full dump; re-running must not duplicate rows,
    so every loader truncates its target table before inserting."""
    zf = _zip_with("f1db-seasons.csv", "year\n2024\n2025\n")
    with patch("ingestion.f1db.loader.ch") as mock_ch:
        from ingestion.f1db import loader
        loader.load_seasons(zf)

    assert mock_ch.truncate_table.call_args[0][0] == "raw_f1db.seasons"
    assert mock_ch.truncate_table.call_count == 1
    # truncate must happen before insert
    truncate_pos = [i for i, c in enumerate(mock_ch.mock_calls) if c[0] == "truncate_table"][0]
    insert_pos = [i for i, c in enumerate(mock_ch.mock_calls) if c[0] == "insert_rows"][0]
    assert truncate_pos < insert_pos


def test_load_race_results_truncates_shared_helper():
    """race_results and sprint_race_results both go through _load_results —
    verify the shared helper truncates too."""
    zf = _zip_with(
        "f1db-races-race-results.csv",
        "year,round,driverId,constructorId,gridPositionNumber,positionNumber,"
        "positionText,positionDisplayOrder,points,laps,reasonRetired,timeMillis\n"
        "2024,1,max-verstappen,red-bull,1,1,1,1,25,57,,5400000\n",
    )
    with patch("ingestion.f1db.loader.ch") as mock_ch:
        from ingestion.f1db import loader
        loader.load_race_results(zf)

    mock_ch.truncate_table.assert_called_once_with("raw_f1db.race_results")
