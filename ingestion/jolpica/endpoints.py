"""One function per Jolpica endpoint. Each returns a list of typed Pydantic models."""

import json

from ingestion.jolpica.client import paginate_all, raw, get_json, BASE_URL, PAGE_SIZE
from ingestion.shared.models import (
    JolpicaSeason, JolpicaCircuit, JolpicaConstructor, JolpicaDriver,
    JolpicaRace, JolpicaResult, JolpicaQualifying, JolpicaSprintResult,
    JolpicaLapTime, JolpicaPitStop, JolpicaDriverStanding, JolpicaConstructorStanding,
)


def get_seasons() -> list[JolpicaSeason]:
    records = paginate_all("seasons", "SeasonTable", "Seasons")
    return [
        JolpicaSeason(year=int(r["season"]), url=r["url"], _raw_json=raw(r))
        for r in records
    ]


def get_circuits() -> list[JolpicaCircuit]:
    records = paginate_all("circuits", "CircuitTable", "Circuits")
    out = []
    for r in records:
        loc = r["Location"]
        out.append(JolpicaCircuit(
            circuit_id=r["circuitId"],
            url=r["url"],
            circuit_name=r["circuitName"],
            locality=loc["locality"],
            country=loc["country"],
            lat=float(loc["lat"]),
            lng=float(loc["long"]),
            alt=float(loc["alt"]) if loc.get("alt") else None,
            _raw_json=raw(r),
        ))
    return out


def get_constructors() -> list[JolpicaConstructor]:
    records = paginate_all("constructors", "ConstructorTable", "Constructors")
    return [
        JolpicaConstructor(
            constructor_id=r["constructorId"],
            url=r["url"],
            name=r["name"],
            nationality=r["nationality"],
            _raw_json=raw(r),
        )
        for r in records
    ]


def get_drivers() -> list[JolpicaDriver]:
    records = paginate_all("drivers", "DriverTable", "Drivers")
    out = []
    for r in records:
        out.append(JolpicaDriver(
            driver_id=r["driverId"],
            url=r.get("url", ""),
            given_name=r.get("givenName", ""),
            family_name=r.get("familyName", ""),
            date_of_birth=r.get("dateOfBirth"),
            nationality=r.get("nationality", ""),
            permanent_number=int(r["permanentNumber"]) if r.get("permanentNumber") else None,
            code=r.get("code"),
            _raw_json=raw(r),
        ))
    return out


def get_races(season: int) -> list[JolpicaRace]:
    records = paginate_all(f"{season}", "RaceTable", "Races")
    return [
        JolpicaRace(
            season=season,
            round=int(r["round"]),
            race_name=r["raceName"],
            circuit_id=r["Circuit"]["circuitId"],
            date=r["date"],
            time=r.get("time"),
            url=r["url"],
            _raw_json=raw(r),
        )
        for r in records
    ]


def get_results(season: int, round_num: int) -> list[JolpicaResult]:
    records = paginate_all(f"{season}/{round_num}/results", "RaceTable", "Races")
    out = []
    for race in records:
        for r in race.get("Results", []):
            out.append(JolpicaResult(
                season=season,
                round=round_num,
                driver_id=r["Driver"]["driverId"],
                constructor_id=r["Constructor"]["constructorId"],
                grid=int(r["grid"]),
                position=int(r["position"]) if r.get("position", "").isdigit() else None,
                position_text=r.get("positionText", ""),
                position_order=int(r["position"]) if r.get("position", "").isdigit() else int(r.get("positionOrder", 0)),
                points=float(r.get("points", 0)),
                laps=int(r.get("laps", 0)),
                status=r.get("status", ""),
                time_millis=int(r["Time"]["millis"]) if r.get("Time") else None,
                fastest_lap_rank=int(r["FastestLap"]["rank"]) if r.get("FastestLap") else None,
                fastest_lap_time=r["FastestLap"]["Time"]["time"] if r.get("FastestLap") else None,
                fastest_lap_speed=float(r["FastestLap"]["AverageSpeed"]["speed"]) if r.get("FastestLap") and r["FastestLap"].get("AverageSpeed") else None,
                _raw_json=raw(r),
            ))
    return out


def get_qualifying(season: int, round_num: int) -> list[JolpicaQualifying]:
    records = paginate_all(f"{season}/{round_num}/qualifying", "RaceTable", "Races")
    out = []
    for race in records:
        for r in race.get("QualifyingResults", []):
            out.append(JolpicaQualifying(
                season=season,
                round=round_num,
                driver_id=r["Driver"]["driverId"],
                constructor_id=r["Constructor"]["constructorId"],
                position=int(r["position"]),
                q1=r.get("Q1"),
                q2=r.get("Q2"),
                q3=r.get("Q3"),
                _raw_json=raw(r),
            ))
    return out


def get_sprint(season: int, round_num: int) -> list[JolpicaSprintResult]:
    records = paginate_all(f"{season}/{round_num}/sprint", "RaceTable", "Races")
    out = []
    for race in records:
        for r in race.get("SprintResults", []):
            out.append(JolpicaSprintResult(
                season=season,
                round=round_num,
                driver_id=r["Driver"]["driverId"],
                constructor_id=r["Constructor"]["constructorId"],
                grid=int(r["grid"]),
                position=int(r["position"]) if r.get("position", "").isdigit() else None,
                position_text=r.get("positionText", ""),
                position_order=int(r["position"]) if r.get("position", "").isdigit() else int(r.get("positionOrder", 0)),
                points=float(r.get("points", 0)),
                laps=int(r.get("laps", 0)),
                status=r.get("status", ""),
                time_millis=int(r["Time"]["millis"]) if r.get("Time") else None,
                _raw_json=raw(r),
            ))
    return out


def get_laps(season: int, round_num: int) -> list[JolpicaLapTime]:
    records = paginate_all(f"{season}/{round_num}/laps", "RaceTable", "Races")
    out = []
    for race in records:
        for lap in race.get("Laps", []):
            lap_num = int(lap["number"])
            for t in lap.get("Timings", []):
                out.append(JolpicaLapTime(
                    season=season,
                    round=round_num,
                    driver_id=t["driverId"],
                    lap=lap_num,
                    position=int(t["position"]),
                    time=t["time"],
                    time_millis=_lap_time_to_ms(t["time"]),
                    _raw_json=raw(t),
                ))
    return out


def get_pit_stops(season: int, round_num: int) -> list[JolpicaPitStop]:
    records = paginate_all(f"{season}/{round_num}/pitstops", "RaceTable", "Races")
    out = []
    for race in records:
        for r in race.get("PitStops", []):
            out.append(JolpicaPitStop(
                season=season,
                round=round_num,
                driver_id=r["driverId"],
                stop=int(r["stop"]),
                lap=int(r["lap"]),
                time=r["time"],
                duration=r["duration"],
                duration_millis=_duration_to_ms(r["duration"]),
                _raw_json=raw(r),
            ))
    return out


def get_driver_standings(season: int, round_num: int) -> list[JolpicaDriverStanding]:
    records = paginate_all(f"{season}/{round_num}/driverstandings", "StandingsTable", "StandingsLists")
    out = []
    for sl in records:
        for r in sl.get("DriverStandings", []):
            out.append(JolpicaDriverStanding(
                season=season,
                round=round_num,
                driver_id=r["Driver"]["driverId"],
                constructor_id=r["Constructors"][0]["constructorId"] if r.get("Constructors") else "",
                position=int(r.get("position", 0)),
                position_text=r.get("positionText", ""),
                points=float(r.get("points", 0)),
                wins=int(r.get("wins", 0)),
                _raw_json=raw(r),
            ))
    return out


def get_constructor_standings(season: int, round_num: int) -> list[JolpicaConstructorStanding]:
    records = paginate_all(f"{season}/{round_num}/constructorstandings", "StandingsTable", "StandingsLists")
    out = []
    for sl in records:
        for r in sl.get("ConstructorStandings", []):
            out.append(JolpicaConstructorStanding(
                season=season,
                round=round_num,
                constructor_id=r["Constructor"]["constructorId"],
                position=int(r.get("position", 0)),
                position_text=r.get("positionText", ""),
                points=float(r.get("points", 0)),
                wins=int(r.get("wins", 0)),
                _raw_json=raw(r),
            ))
    return out


# ── Helpers ───────────────────────────────────────────────────────────────────

def _lap_time_to_ms(t: str) -> int:
    """Convert 'M:SS.mmm' or 'SS.mmm' to milliseconds."""
    if ":" in t:
        minutes, rest = t.split(":", 1)
        seconds, ms = rest.split(".")
        return int(minutes) * 60_000 + int(seconds) * 1_000 + int(ms)
    seconds, ms = t.split(".")
    return int(seconds) * 1_000 + int(ms)


def _duration_to_ms(d: str) -> int | None:
    try:
        return _lap_time_to_ms(d)
    except Exception:
        return None
