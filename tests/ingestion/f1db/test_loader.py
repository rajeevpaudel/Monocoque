import io
import zipfile
from unittest.mock import patch


def _zip_with(name: str, csv_text: str) -> zipfile.ZipFile:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(name, csv_text)
    buf.seek(0)
    return zipfile.ZipFile(buf)


def test_load_seasons_atomically_reloads():
    """Each f1db release is a full dump; re-running must not duplicate rows and a
    mid-load failure must not leave the table empty/partial, so every loader goes
    through ch.atomic_reload() (shadow table + EXCHANGE TABLES) rather than a bare
    truncate-then-insert."""
    zf = _zip_with("f1db-seasons.csv", "year\n2024\n2025\n")
    with patch("ingestion.f1db.loader.ch") as mock_ch:
        from ingestion.f1db import loader

        loader.load_seasons(zf)

    mock_ch.atomic_reload.assert_called_once()
    table_arg, rows_arg = mock_ch.atomic_reload.call_args[0][:2]
    assert table_arg == "raw_f1db.seasons"
    assert rows_arg == [{"year": 2024}, {"year": 2025}]
    # loader must not fall back to the old truncate-then-insert pattern
    mock_ch.truncate_table.assert_not_called()


def test_load_race_results_atomically_reloads_shared_helper():
    """race_results and sprint_race_results both go through _load_results —
    verify the shared helper uses atomic_reload too."""
    zf = _zip_with(
        "f1db-races-race-results.csv",
        "year,round,driverId,constructorId,gridPositionNumber,positionNumber,"
        "positionText,positionDisplayOrder,points,laps,reasonRetired,timeMillis\n"
        "2024,1,max-verstappen,red-bull,1,1,1,1,25,57,,5400000\n",
    )
    with patch("ingestion.f1db.loader.ch") as mock_ch:
        from ingestion.f1db import loader

        loader.load_race_results(zf)

    table_arg = mock_ch.atomic_reload.call_args[0][0]
    assert table_arg == "raw_f1db.race_results"
    mock_ch.truncate_table.assert_not_called()


def test_download_rejects_malformed_version():
    """version flows unvalidated into a URL passed to urllib.request.urlopen;
    reject anything that isn't a plain 'vX.Y.Z'-style release tag before it's used."""
    import pytest

    from ingestion.f1db import loader

    with pytest.raises(ValueError):
        loader._download("not-a-real-release")


def test_download_accepts_well_formed_version():
    from unittest.mock import MagicMock

    from ingestion.f1db import loader

    fake_response = MagicMock()
    fake_response.__enter__.return_value.read.return_value = (
        b"PK\x05\x06" + b"\x00" * 18
    )  # empty zip
    with patch("ingestion.f1db.loader.urllib.request.urlopen", return_value=fake_response):
        zf = loader._download("v2026.6.1")
    assert zf.namelist() == []
