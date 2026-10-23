from unittest.mock import patch

from ingestion.openf1 import driver_map as dm


def _races_page(results):
    return [{"Results": results}]


def _drivers_page(drivers):
    return drivers


def test_jolpica_number_map_prefers_race_result_number():
    with patch("ingestion.openf1.driver_map.paginate_all") as mock_paginate:
        mock_paginate.side_effect = [
            _races_page([{"number": "1", "Driver": {"driverId": "verstappen"}}]),
            _drivers_page([]),
        ]
        mapping = dm._jolpica_number_map(2024)
    assert mapping == {1: "verstappen"}


def test_jolpica_number_map_supplements_with_permanent_number_for_unmapped_driver():
    """A reserve/practice-only driver with no race result still needs an entry,
    sourced from their permanent number in the drivers endpoint."""
    with patch("ingestion.openf1.driver_map.paginate_all") as mock_paginate:
        mock_paginate.side_effect = [
            _races_page([{"number": "1", "Driver": {"driverId": "verstappen"}}]),
            _drivers_page([{"driverId": "bearman", "permanentNumber": "87"}]),
        ]
        mapping = dm._jolpica_number_map(2024)
    assert mapping == {1: "verstappen", 87: "bearman"}


def test_jolpica_number_map_skips_permanent_number_for_already_mapped_driver():
    """A champion racing on #1 has a different permanent number (e.g. #33/#1
    for Verstappen) — the permanent-number pass must not add a second,
    conflicting entry for a driver already resolved via race results."""
    with patch("ingestion.openf1.driver_map.paginate_all") as mock_paginate:
        mock_paginate.side_effect = [
            _races_page([{"number": "1", "Driver": {"driverId": "verstappen"}}]),
            _drivers_page([{"driverId": "verstappen", "permanentNumber": "33"}]),
        ]
        mapping = dm._jolpica_number_map(2024)
    assert mapping == {1: "verstappen"}
    assert 33 not in mapping


def test_jolpica_number_map_skips_permanent_number_when_number_already_taken():
    with patch("ingestion.openf1.driver_map.paginate_all") as mock_paginate:
        mock_paginate.side_effect = [
            _races_page([{"number": "44", "Driver": {"driverId": "hamilton"}}]),
            _drivers_page([{"driverId": "someone_else", "permanentNumber": "44"}]),
        ]
        mapping = dm._jolpica_number_map(2024)
    assert mapping == {44: "hamilton"}


def test_jolpica_number_map_ignores_results_without_a_number():
    with patch("ingestion.openf1.driver_map.paginate_all") as mock_paginate:
        mock_paginate.side_effect = [
            _races_page([{"Driver": {"driverId": "unknown"}}]),
            _drivers_page([]),
        ]
        mapping = dm._jolpica_number_map(2024)
    assert mapping == {}


def test_jolpica_number_map_ignores_drivers_without_permanent_number():
    with patch("ingestion.openf1.driver_map.paginate_all") as mock_paginate:
        mock_paginate.side_effect = [
            _races_page([]),
            _drivers_page([{"driverId": "reserve_driver"}]),
        ]
        mapping = dm._jolpica_number_map(2024)
    assert mapping == {}


def test_build_driver_map_deletes_year_then_inserts():
    with (
        patch(
            "ingestion.openf1.driver_map._jolpica_number_map",
            return_value={1: "verstappen", 44: "hamilton"},
        ),
        patch("ingestion.openf1.driver_map.ch") as mock_ch,
    ):
        dm.build_driver_map(2024)

    mock_ch.delete_rows.assert_called_once_with(
        "dim.driver_id_map", "season = {year:UInt16}", parameters={"year": 2024}
    )
    mock_ch.insert_rows.assert_called_once()
    table_arg, rows_arg = mock_ch.insert_rows.call_args[0]
    assert table_arg == "dim.driver_id_map"
    assert {r["openf1_driver_number"] for r in rows_arg} == {1, 44}
    assert all(r["season"] == 2024 for r in rows_arg)


def test_build_driver_map_skips_write_when_no_rows():
    with (
        patch("ingestion.openf1.driver_map._jolpica_number_map", return_value={}),
        patch("ingestion.openf1.driver_map.ch") as mock_ch,
    ):
        dm.build_driver_map(2024)

    mock_ch.delete_rows.assert_not_called()
    mock_ch.insert_rows.assert_not_called()


def test_main_parses_args_and_delegates():
    import sys

    argv = ["prog", "--year", "2024"]
    with (
        patch.object(sys, "argv", argv),
        patch("ingestion.openf1.driver_map.build_driver_map") as mock_build,
    ):
        dm.main()
    mock_build.assert_called_once_with(2024)
