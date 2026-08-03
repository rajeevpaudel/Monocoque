from unittest.mock import MagicMock, patch

import pytest

from ingestion.shared import clickhouse as ch


@pytest.fixture(autouse=True)
def _reset_client_singleton():
    """ch._client is a module-level singleton; reset it around every test so
    tests don't leak a MagicMock client into each other."""
    ch._client = None
    yield
    ch._client = None


def test_get_client_builds_once_and_caches():
    with patch("ingestion.shared.clickhouse._get_client") as mock_build:
        mock_build.return_value = MagicMock()
        first = ch.get_client()
        second = ch.get_client()
    mock_build.assert_called_once()
    assert first is second


def test_get_client_reads_env(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "chost")
    monkeypatch.setenv("CLICKHOUSE_PORT", "9000")
    monkeypatch.setenv("CLICKHOUSE_USER", "cuser")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "cpass")
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "cdb")
    with patch("ingestion.shared.clickhouse.clickhouse_connect") as mock_cc:
        ch._get_client()
    mock_cc.get_client.assert_called_once_with(
        host="chost", port=9000, username="cuser", password="cpass", database="cdb"
    )


def test_get_client_defaults_when_env_unset(monkeypatch):
    for var in ("CLICKHOUSE_HOST", "CLICKHOUSE_PORT", "CLICKHOUSE_USER",
                "CLICKHOUSE_PASSWORD", "CLICKHOUSE_DATABASE"):
        monkeypatch.delenv(var, raising=False)
    with patch("ingestion.shared.clickhouse.clickhouse_connect") as mock_cc:
        ch._get_client()
    mock_cc.get_client.assert_called_once_with(
        host="localhost", port=8123, username="default", password="", database="default"
    )


def test_insert_rows_noop_on_empty_list():
    with patch("ingestion.shared.clickhouse.get_client") as mock_get_client:
        ch.insert_rows("some.table", [])
    mock_get_client.assert_not_called()


def test_insert_rows_builds_columns_from_first_row():
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        ch.insert_rows("some.table", [{"a": 1, "b": 2}, {"a": 3, "b": 4}])
    mock_client.insert.assert_called_once_with(
        "some.table", [[1, 2], [3, 4]], column_names=["a", "b"]
    )


def test_insert_arrow_noop_on_empty_table():
    arrow_table = MagicMock()
    arrow_table.num_rows = 0
    with patch("ingestion.shared.clickhouse.get_client") as mock_get_client:
        ch.insert_arrow("some.table", arrow_table)
    mock_get_client.assert_not_called()


def test_insert_arrow_inserts_nonempty_table():
    arrow_table = MagicMock()
    arrow_table.num_rows = 5
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        ch.insert_arrow("some.table", arrow_table)
    mock_client.insert_arrow.assert_called_once_with("some.table", arrow_table)


def test_delete_rows_issues_delete_command():
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        ch.delete_rows("some.table", "season = 2024")
    mock_client.command.assert_called_once_with(
        "DELETE FROM some.table WHERE season = 2024", parameters={}
    )


def test_delete_rows_passes_parameters():
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        ch.delete_rows("some.table", "season = {season:UInt16}", parameters={"season": 2024})
    mock_client.command.assert_called_once_with(
        "DELETE FROM some.table WHERE season = {season:UInt16}", parameters={"season": 2024}
    )


def test_truncate_table_issues_truncate_command():
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        ch.truncate_table("some.table")
    mock_client.command.assert_called_once_with("TRUNCATE TABLE IF EXISTS some.table")


def test_query_one_returns_first_value_of_first_row():
    mock_client = MagicMock()
    mock_client.query.return_value.result_rows = [[42, "ignored"]]
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        result = ch.query_one("SELECT 1")
    assert result == 42


def test_query_one_returns_none_when_no_rows():
    mock_client = MagicMock()
    mock_client.query.return_value.result_rows = []
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        result = ch.query_one("SELECT 1")
    assert result is None


def test_query_one_passes_parameters():
    mock_client = MagicMock()
    mock_client.query.return_value.result_rows = [[1]]
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client):
        ch.query_one("SELECT {x:Int32}", parameters={"x": 1})
    mock_client.query.assert_called_once_with("SELECT {x:Int32}", parameters={"x": 1})


def test_atomic_reload_creates_shadow_and_swaps_on_success():
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client), \
         patch("ingestion.shared.clickhouse.insert_rows") as mock_insert:
        total = ch.atomic_reload("real.table", [{"a": 1}, {"a": 2}])

    assert total == 2
    commands = [c.args[0] for c in mock_client.command.call_args_list]
    assert "DROP TABLE IF EXISTS real.table_reload_staging" in commands[0]
    assert "CREATE TABLE real.table_reload_staging AS real.table" in commands[1]
    assert "EXCHANGE TABLES real.table AND real.table_reload_staging" in commands[2]
    assert "DROP TABLE IF EXISTS real.table_reload_staging" in commands[3]
    mock_insert.assert_called_once_with("real.table_reload_staging", [{"a": 1}, {"a": 2}])


def test_atomic_reload_chunks_inserts():
    mock_client = MagicMock()
    rows = [{"a": i} for i in range(12)]
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client), \
         patch("ingestion.shared.clickhouse.insert_rows") as mock_insert:
        total = ch.atomic_reload("real.table", rows, chunk_size=5)

    assert total == 12
    assert mock_insert.call_count == 3
    chunk_lengths = [len(c.args[1]) for c in mock_insert.call_args_list]
    assert chunk_lengths == [5, 5, 2]


def test_atomic_reload_noop_rows_still_swaps_empty_shadow():
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client), \
         patch("ingestion.shared.clickhouse.insert_rows") as mock_insert:
        total = ch.atomic_reload("real.table", [])

    assert total == 0
    mock_insert.assert_not_called()
    commands = [c.args[0] for c in mock_client.command.call_args_list]
    assert any("EXCHANGE TABLES" in c for c in commands)


def test_atomic_reload_drops_shadow_and_reraises_on_failure():
    """A mid-load failure must leave the real table untouched — no EXCHANGE, shadow dropped."""
    mock_client = MagicMock()
    with patch("ingestion.shared.clickhouse.get_client", return_value=mock_client), \
         patch("ingestion.shared.clickhouse.insert_rows", side_effect=RuntimeError("boom")), \
         pytest.raises(RuntimeError):
        ch.atomic_reload("real.table", [{"a": 1}])

    commands = [c.args[0] for c in mock_client.command.call_args_list]
    assert not any("EXCHANGE TABLES" in c for c in commands)
    drop_commands = [c for c in commands if "DROP TABLE IF EXISTS real.table_reload_staging" in c]
    assert len(drop_commands) == 2  # once up front, once in the except block
