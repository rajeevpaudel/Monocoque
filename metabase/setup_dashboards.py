"""Provision Metabase: admin user, ClickHouse connection, and the Monocoque dashboards.

Idempotent — re-running updates cards/dashboards in place (matched by name).

    MB_ADMIN_PASSWORD=... python metabase/setup_dashboards.py

Environment:
    MB_URL             Metabase base URL              (default http://localhost:3001)
    MB_ADMIN_EMAIL     admin login                    (default admin@monocoque.local)
    MB_ADMIN_PASSWORD  admin password                 (required; used for first-run setup too)
    MB_CH_HOST         ClickHouse host as seen by the Metabase container (default clickhouse)
    MB_CH_PORT / CLICKHOUSE_USER / CLICKHOUSE_PASSWORD
"""

import os
import sys
import uuid

import httpx

MB_URL = os.environ.get("MB_URL", "http://localhost:3001").rstrip("/")
EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@monocoque.local")
PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "")
DB_NAME = "F1 Warehouse"
COLLECTION = "Monocoque F1"

CH_DETAILS = {
    "host": os.environ.get("MB_CH_HOST", "clickhouse"),
    "port": int(os.environ.get("MB_CH_PORT", 8123)),
    "user": os.environ.get("CLICKHOUSE_USER", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    "dbname": "f1_mart",
    "ssl": False,
}

# ── Card definitions ──────────────────────────────────────────────────────────
# params: template tags used by the SQL ({{season}}, {{round}}), wired to dashboard filters.


def _bar(x, y, stacked=False):
    s = {"graph.dimensions": x if isinstance(x, list) else [x], "graph.metrics": [y]}
    if stacked:
        s["stackable.stack_type"] = "stacked"
    return s


def _line(x, series, y):
    return {"graph.dimensions": [x, series], "graph.metrics": [y]}


CARDS = {
    # Championship
    "Driver standings": dict(
        params=["season"],
        display="table",
        sql="""SELECT championship_position AS pos, driver_name, final_constructor_id AS team,
       points, wins, podiums, poles, fastest_laps, dnfs, sprint_points, points_per_start
FROM f1_mart.mart_driver_season_standings WHERE season = {{season}}
ORDER BY ifNull(championship_position, 999), points DESC""",
    ),
    "Constructor standings": dict(
        params=["season"],
        display="bar",
        viz=_bar("constructor_name", "points"),
        sql="""SELECT constructor_name, points
FROM f1_mart.mart_constructor_season_standings WHERE season = {{season}}
ORDER BY points DESC""",
    ),
    "Championship points progression (top 6)": dict(
        params=["season"],
        display="line",
        viz=_line("round", "driver", "points"),
        sql="""SELECT s.round AS round, d.driver_name AS driver, s.points AS points
FROM f1_mart.mart_standings s
JOIN (SELECT driver_id, driver_name FROM f1_mart.mart_driver_season_standings
      WHERE season = {{season}} ORDER BY points DESC LIMIT 6) d ON d.driver_id = s.driver_id
WHERE s.season = {{season}}
ORDER BY round""",
    ),
    "Team-mate head-to-head": dict(
        params=["season"],
        display="table",
        sql="""SELECT constructor_id AS team, driver_id, teammate_id, races_together,
       quali_wins || '-' || quali_losses AS quali_h2h, race_wins || '-' || race_losses AS race_h2h,
       points, teammate_points, avg_quali_position_delta
FROM f1_mart.mart_teammate_head_to_head
WHERE season = {{season}} AND driver_id < teammate_id
ORDER BY team, races_together DESC""",
    ),
    "Driver line-up changes": dict(
        params=["season"],
        display="table",
        sql="""SELECT driver_name, constructor_name, start_season, start_round, end_season, end_round,
       races_entered, stint_type, started_mid_season, ended_mid_season
FROM f1_mart.dim_driver_team_stints
WHERE {{season}} BETWEEN start_season AND end_season
  AND (started_mid_season = 1 OR ended_mid_season = 1 OR stint_type IN ('one_off', 'short_term'))
ORDER BY constructor_name, start_season, start_round""",
    ),
    # Race analysis
    "Race pace — gap to fastest (s)": dict(
        params=["season", "round"],
        display="bar",
        viz=_bar("driver_id", "gap_to_best_s"),
        sql="""SELECT driver_id, gap_to_best_s FROM f1_mart.mart_race_pace
WHERE season = {{season}} AND round = {{round}} ORDER BY pace_rank""",
    ),
    "Tyre degradation by compound (s/lap)": dict(
        params=["season", "round"],
        display="bar",
        viz=_bar("compound", "avg_deg_s_per_lap"),
        sql="""SELECT compound, round(avg(deg_s_per_lap), 4) AS avg_deg_s_per_lap, count() AS stints
FROM f1_mart.mart_tyre_degradation
WHERE season = {{season}} AND round = {{round}} AND deg_s_per_lap IS NOT NULL
GROUP BY compound ORDER BY avg_deg_s_per_lap""",
    ),
    "Tyre stints": dict(
        params=["season", "round"],
        display="table",
        sql="""SELECT driver_id, stint_number, compound, first_lap, last_lap, clean_laps,
       median_lap_s, deg_s_per_lap
FROM f1_mart.mart_tyre_degradation WHERE season = {{season}} AND round = {{round}}
ORDER BY driver_id, stint_number""",
    ),
    "Positions gained": dict(
        params=["season", "round"],
        display="bar",
        viz=_bar("driver_id", "positions_gained"),
        sql="""SELECT driver_id, positions_gained FROM f1_mart.mart_position_changes
WHERE season = {{season}} AND round = {{round}} AND positions_gained IS NOT NULL
ORDER BY positions_gained DESC""",
    ),
    "Race position summary": dict(
        params=["season", "round"],
        display="table",
        sql="""SELECT driver_id, grid_position, lap1_position, finish_position, positions_gained,
       laps_led, on_track_gains, on_track_losses, live_position_gains
FROM f1_mart.mart_position_changes WHERE season = {{season}} AND round = {{round}}
ORDER BY ifNull(finish_position, 99)""",
    ),
    "Session weather": dict(
        params=["season", "round"],
        display="table",
        sql="""SELECT session_name, is_wet, rain_sample_share, wet_tyre_lap_share, avg_air_temp_c,
       avg_track_temp_c, avg_humidity_pct, pace_delta_vs_dry_pct
FROM f1_mart.mart_weather_impact WHERE season = {{season}} AND round = {{round}}
ORDER BY date_start""",
    ),
    # Calendar & history
    "Upcoming races": dict(
        params=[],
        display="table",
        sql="""SELECT season, round, race_name, circuit_name, race_date, is_sprint_weekend, status
FROM f1_mart.dim_calendar WHERE status != 'completed' ORDER BY race_date LIMIT 30""",
    ),
    "World champions": dict(
        params=[],
        display="table",
        sql="""SELECT d.season AS season, d.driver_name AS driver_champion, d.points AS points,
       d.wins AS wins, c.constructor_name AS constructor_champion
FROM f1_mart.mart_driver_season_standings d
LEFT JOIN (SELECT season, constructor_name FROM f1_mart.mart_constructor_season_standings
           WHERE is_champion = 1) c ON c.season = d.season
WHERE d.is_champion = 1 ORDER BY season DESC""",
    ),
    "Wet vs dry sessions": dict(
        params=[],
        display="table",
        sql="""SELECT season, race_name, session_name, rain_sample_share, wet_tyre_lap_share,
       avg_track_temp_c, pace_delta_vs_dry_pct
FROM f1_mart.mart_weather_impact WHERE is_wet = 1 ORDER BY date_start DESC""",
    ),
}

# dashboard -> (filters, [(card, col, row, width, height)])  — 24-column grid
DASHBOARDS = {
    "F1 Championship": (
        ["season"],
        [
            ("Driver standings", 0, 0, 14, 9),
            ("Constructor standings", 14, 0, 10, 9),
            ("Championship points progression (top 6)", 0, 9, 24, 8),
            ("Team-mate head-to-head", 0, 17, 14, 8),
            ("Driver line-up changes", 14, 17, 10, 8),
        ],
    ),
    "F1 Race Analysis": (
        ["season", "round"],
        [
            ("Race pace — gap to fastest (s)", 0, 0, 12, 8),
            ("Positions gained", 12, 0, 12, 8),
            ("Tyre degradation by compound (s/lap)", 0, 8, 8, 7),
            ("Tyre stints", 8, 8, 16, 7),
            ("Race position summary", 0, 15, 16, 8),
            ("Session weather", 16, 15, 8, 8),
        ],
    ),
    "F1 Calendar & History": (
        [],
        [
            ("Upcoming races", 0, 0, 12, 9),
            ("World champions", 12, 0, 12, 9),
            ("Wet vs dry sessions", 0, 9, 24, 8),
        ],
    ),
}

DEFAULTS = {"season": 2024, "round": 1}


class Metabase:
    def __init__(self, url: str):
        self.http = httpx.Client(base_url=url, timeout=60)

    def call(self, method: str, path: str, **kw):
        resp = self.http.request(method, f"/api{path}", **kw)
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} → {resp.status_code}: {resp.text[:500]}")
        return resp.json() if resp.content else None

    def ensure_session(self) -> None:
        props = self.call("GET", "/session/properties")
        if not props.get("has-user-setup"):
            print("First run: creating admin user")
            self.call(
                "POST",
                "/setup",
                json={
                    "token": props["setup-token"],
                    "user": {
                        "email": EMAIL,
                        "password": PASSWORD,
                        "first_name": "Monocoque",
                        "last_name": "Admin",
                        "site_name": "Monocoque",
                    },
                    "prefs": {"site_name": "Monocoque", "allow_tracking": False},
                },
            )
        sid = self.call("POST", "/session", json={"username": EMAIL, "password": PASSWORD})["id"]
        self.http.headers["X-Metabase-Session"] = sid

    def ensure_database(self) -> int:
        dbs = self.call("GET", "/database")
        dbs = dbs["data"] if isinstance(dbs, dict) else dbs
        existing = next((d for d in dbs if d["name"] == DB_NAME), None)
        body = {"name": DB_NAME, "engine": "clickhouse", "details": CH_DETAILS}
        if existing:
            self.call("PUT", f"/database/{existing['id']}", json=body)
            db_id = existing["id"]
        else:
            db_id = self.call("POST", "/database", json=body)["id"]
            print(f"Added database '{DB_NAME}' (id {db_id})")
        self.call("POST", f"/database/{db_id}/sync_schema")
        return db_id

    def ensure_collection(self) -> int:
        for c in self.call("GET", "/collection"):
            if c.get("name") == COLLECTION and not c.get("archived"):
                return c["id"]
        return self.call("POST", "/collection", json={"name": COLLECTION, "color": "#E10600"})["id"]

    def _items(self, collection_id: int, model: str) -> dict[str, int]:
        items = self.call("GET", f"/collection/{collection_id}/items", params={"models": model})
        items = items["data"] if isinstance(items, dict) else items
        return {i["name"]: i["id"] for i in items}

    def upsert_cards(self, db_id: int, collection_id: int) -> dict[str, int]:
        existing = self._items(collection_id, "card")
        ids = {}
        for name, spec in CARDS.items():
            tags = {
                p: {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_URL, p)),
                    "name": p,
                    "display-name": p.title(),
                    "type": "number",
                    "required": True,
                    "default": str(DEFAULTS[p]),
                }
                for p in spec["params"]
            }
            body = {
                "name": name,
                "collection_id": collection_id,
                "display": spec["display"],
                "visualization_settings": spec.get("viz", {}),
                "dataset_query": {
                    "type": "native",
                    "database": db_id,
                    "native": {"query": spec["sql"], "template-tags": tags},
                },
            }
            if name in existing:
                self.call("PUT", f"/card/{existing[name]}", json=body)
                ids[name] = existing[name]
            else:
                ids[name] = self.call("POST", "/card", json=body)["id"]
        return ids

    def upsert_dashboards(self, collection_id: int, card_ids: dict[str, int]) -> None:
        existing = self._items(collection_id, "dashboard")
        for name, (filters, layout) in DASHBOARDS.items():
            dash_id = (
                existing.get(name)
                or self.call(
                    "POST", "/dashboard", json={"name": name, "collection_id": collection_id}
                )["id"]
            )
            parameters = [
                {
                    "id": p,
                    "name": p.title(),
                    "slug": p,
                    "type": "number/=",
                    "sectionId": "number",
                    "default": [DEFAULTS[p]],
                }
                for p in filters
            ]
            dashcards = []
            for i, (card, col, row, w, h) in enumerate(layout):
                card_id = card_ids[card]
                dashcards.append(
                    {
                        "id": -(i + 1),
                        "card_id": card_id,
                        "col": col,
                        "row": row,
                        "size_x": w,
                        "size_y": h,
                        "parameter_mappings": [
                            {
                                "parameter_id": p,
                                "card_id": card_id,
                                "target": ["variable", ["template-tag", p]],
                            }
                            for p in CARDS[card]["params"]
                            if p in filters
                        ],
                    }
                )
            self.call(
                "PUT",
                f"/dashboard/{dash_id}",
                json={"parameters": parameters, "dashcards": dashcards},
            )
            print(f"Dashboard '{name}': {MB_URL}/dashboard/{dash_id}")


def main() -> None:
    if not PASSWORD:
        sys.exit("Set MB_ADMIN_PASSWORD (used to create or log in as the Metabase admin).")
    mb = Metabase(MB_URL)
    mb.ensure_session()
    db_id = mb.ensure_database()
    collection_id = mb.ensure_collection()
    card_ids = mb.upsert_cards(db_id, collection_id)
    mb.upsert_dashboards(collection_id, card_ids)


if __name__ == "__main__":
    main()
