from unittest.mock import patch

from ingestion.jolpica import endpoints as ep


def _patched(records):
    return patch("ingestion.jolpica.endpoints.paginate_all", return_value=records)


# ── _lap_time_to_ms / _duration_to_ms ───────────────────────────────────────


def test_lap_time_to_ms_with_minutes():
    assert ep._lap_time_to_ms("1:23.456") == 83_456


def test_lap_time_to_ms_without_minutes():
    assert ep._lap_time_to_ms("23.456") == 23_456


def test_duration_to_ms_valid():
    assert ep._duration_to_ms("23.456") == 23_456


def test_duration_to_ms_invalid_returns_none():
    assert ep._duration_to_ms("N/A") is None


# ── get_seasons / get_circuits / get_constructors ───────────────────────────


def test_get_seasons():
    with _patched([{"season": "2024", "url": "https://x"}]):
        out = ep.get_seasons()
    assert out[0].year == 2024
    assert out[0].url == "https://x"


def test_get_circuits_full_fields():
    record = {
        "circuitId": "monza",
        "url": "https://x",
        "circuitName": "Monza",
        "Location": {
            "locality": "Monza",
            "country": "Italy",
            "lat": "45.6",
            "long": "9.28",
            "alt": "162",
        },
        "length": "5.793",
        "turns": "11",
    }
    with _patched([record]):
        out = ep.get_circuits()
    c = out[0]
    assert c.circuit_id == "monza"
    assert c.alt == 162.0
    assert c.length_km == 5.793
    assert c.corners == 11


def test_get_circuits_missing_optional_fields():
    record = {
        "circuitId": "monza",
        "url": "https://x",
        "circuitName": "Monza",
        "Location": {"locality": "Monza", "country": "Italy", "lat": "45.6", "long": "9.28"},
    }
    with _patched([record]):
        out = ep.get_circuits()
    c = out[0]
    assert c.alt is None
    assert c.length_km is None
    assert c.corners is None


def test_get_constructors():
    with _patched(
        [
            {
                "constructorId": "ferrari",
                "url": "https://x",
                "name": "Ferrari",
                "nationality": "Italian",
            }
        ]
    ):
        out = ep.get_constructors()
    assert out[0].constructor_id == "ferrari"


def test_get_drivers_defaults_missing_fields():
    with _patched([{"driverId": "hulkenberg"}]):
        out = ep.get_drivers()
    d = out[0]
    assert d.url == ""
    assert d.given_name == ""
    assert d.permanent_number is None
    assert d.code is None


def test_get_drivers_with_permanent_number():
    with _patched([{"driverId": "verstappen", "permanentNumber": "1", "code": "VER"}]):
        out = ep.get_drivers()
    assert out[0].permanent_number == 1
    assert out[0].code == "VER"


# ── get_races ────────────────────────────────────────────────────────────────


def test_get_races():
    record = {
        "round": "4",
        "raceName": "Miami GP",
        "Circuit": {"circuitId": "miami"},
        "date": "2024-05-05",
        "url": "https://x",
    }
    with _patched([record]):
        out = ep.get_races(2024)
    r = out[0]
    assert r.season == 2024
    assert r.round == 4
    assert r.circuit_id == "miami"
    assert r.time is None


# ── get_results ──────────────────────────────────────────────────────────────


def _race_wrapper(results):
    return [{"Results": results}]


def test_get_results_finished_driver():
    result = {
        "Driver": {"driverId": "verstappen"},
        "Constructor": {"constructorId": "red_bull"},
        "grid": "1",
        "position": "1",
        "positionText": "1",
        "points": "25",
        "laps": "57",
        "status": "Finished",
        "Time": {"millis": "5400000"},
        "FastestLap": {
            "rank": "1",
            "Time": {"time": "1:23.456"},
            "AverageSpeed": {"speed": "220.5"},
        },
    }
    with _patched(_race_wrapper([result])):
        out = ep.get_results(2024, 1)
    r = out[0]
    assert r.position == 1
    assert r.position_order == 1
    assert r.time_millis == 5_400_000
    assert r.fastest_lap_rank == 1
    assert r.fastest_lap_time == "1:23.456"
    assert r.fastest_lap_speed == 220.5


def test_get_results_dnf_driver_has_null_position():
    result = {
        "Driver": {"driverId": "sargeant"},
        "Constructor": {"constructorId": "williams"},
        "grid": "20",
        "position": "R",
        "positionText": "R",
        "positionOrder": "20",
        "points": "0",
        "laps": "10",
        "status": "Retired",
    }
    with _patched(_race_wrapper([result])):
        out = ep.get_results(2024, 1)
    r = out[0]
    assert r.position is None
    assert r.position_order == 20
    assert r.time_millis is None
    assert r.fastest_lap_rank is None
    assert r.fastest_lap_speed is None


def test_get_results_fastest_lap_without_average_speed():
    result = {
        "Driver": {"driverId": "verstappen"},
        "Constructor": {"constructorId": "red_bull"},
        "grid": "1",
        "position": "1",
        "positionText": "1",
        "points": "25",
        "laps": "57",
        "status": "Finished",
        "FastestLap": {"rank": "1", "Time": {"time": "1:23.456"}},
    }
    with _patched(_race_wrapper([result])):
        out = ep.get_results(2024, 1)
    assert out[0].fastest_lap_speed is None
    assert out[0].fastest_lap_time == "1:23.456"


# ── get_qualifying ───────────────────────────────────────────────────────────


def test_get_qualifying():
    record = {
        "Driver": {"driverId": "leclerc"},
        "Constructor": {"constructorId": "ferrari"},
        "position": "1",
        "Q1": "1:20.1",
        "Q2": "1:19.5",
        "Q3": "1:19.0",
    }
    with _patched([{"QualifyingResults": [record]}]):
        out = ep.get_qualifying(2024, 1)
    q = out[0]
    assert q.position == 1
    assert q.q3 == "1:19.0"


def test_get_qualifying_missing_q3_for_eliminated_driver():
    record = {
        "Driver": {"driverId": "sargeant"},
        "Constructor": {"constructorId": "williams"},
        "position": "18",
        "Q1": "1:22.1",
    }
    with _patched([{"QualifyingResults": [record]}]):
        out = ep.get_qualifying(2024, 1)
    assert out[0].q2 is None
    assert out[0].q3 is None


# ── get_sprint ───────────────────────────────────────────────────────────────


def test_get_sprint():
    record = {
        "Driver": {"driverId": "norris"},
        "Constructor": {"constructorId": "mclaren"},
        "grid": "2",
        "position": "1",
        "positionText": "1",
        "points": "8",
        "laps": "19",
        "status": "Finished",
        "Time": {"millis": "1800000"},
    }
    with _patched([{"SprintResults": [record]}]):
        out = ep.get_sprint(2024, 5)
    assert out[0].driver_id == "norris"
    assert out[0].time_millis == 1_800_000


# ── get_laps ─────────────────────────────────────────────────────────────────


def test_get_laps_expands_timings_per_driver():
    race = {
        "Laps": [
            {
                "number": "1",
                "Timings": [
                    {"driverId": "verstappen", "position": "1", "time": "1:35.123"},
                    {"driverId": "hamilton", "position": "2", "time": "1:36.456"},
                ],
            }
        ]
    }
    with _patched([race]):
        out = ep.get_laps(2024, 1)
    assert len(out) == 2
    assert out[0].lap == 1
    assert out[0].time_millis == 95_123
    assert out[1].driver_id == "hamilton"


# ── get_pit_stops ────────────────────────────────────────────────────────────


def test_get_pit_stops():
    race = {
        "PitStops": [
            {
                "driverId": "leclerc",
                "stop": "1",
                "lap": "15",
                "time": "14:23:15",
                "duration": "2.345",
            }
        ]
    }
    with _patched([race]):
        out = ep.get_pit_stops(2024, 1)
    assert out[0].duration_millis == 2_345


# ── get_driver_standings / get_constructor_standings ────────────────────────


def test_get_driver_standings():
    sl = {
        "DriverStandings": [
            {
                "Driver": {"driverId": "verstappen"},
                "Constructors": [{"constructorId": "red_bull"}],
                "position": "1",
                "positionText": "1",
                "points": "575",
                "wins": "19",
            }
        ]
    }
    with _patched([sl]):
        out = ep.get_driver_standings(2024, 24)
    assert out[0].constructor_id == "red_bull"
    assert out[0].wins == 19


def test_get_driver_standings_no_constructor_listed():
    sl = {
        "DriverStandings": [
            {
                "Driver": {"driverId": "x"},
                "position": "20",
                "positionText": "20",
                "points": "0",
                "wins": "0",
            }
        ]
    }
    with _patched([sl]):
        out = ep.get_driver_standings(2024, 1)
    assert out[0].constructor_id == ""


def test_get_constructor_standings():
    sl = {
        "ConstructorStandings": [
            {
                "Constructor": {"constructorId": "red_bull"},
                "position": "1",
                "positionText": "1",
                "points": "860",
                "wins": "21",
            }
        ]
    }
    with _patched([sl]):
        out = ep.get_constructor_standings(2024, 24)
    assert out[0].constructor_id == "red_bull"
    assert out[0].points == 860.0
