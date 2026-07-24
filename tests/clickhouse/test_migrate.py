import fcntl

from clickhouse.migrate import _LOCK_PATH, _strip_comments, run_migrations, split_statements


def test_split_statements_basic():
    sql = "SELECT 1;\nSELECT 2;"
    stmts = [s.strip() for s in split_statements(sql) if s.strip()]
    assert stmts == ["SELECT 1", "SELECT 2"]


def test_split_statements_ignores_semicolon_inside_string_literal():
    sql = "INSERT INTO t (name) VALUES ('a;b');\nSELECT 1;"
    stmts = [s.strip() for s in split_statements(sql) if s.strip()]
    assert len(stmts) == 2
    assert "'a;b'" in stmts[0]


def test_split_statements_ignores_semicolon_in_comment():
    sql = "-- do this; then that\nSELECT 1;"
    stmts = [s.strip() for s in split_statements(sql) if s.strip()]
    assert len(stmts) == 1


def test_split_statements_handles_doubled_quote_escape():
    sql = "INSERT INTO t (name) VALUES ('it''s; fine');\nSELECT 1;"
    stmts = [s.strip() for s in split_statements(sql) if s.strip()]
    assert len(stmts) == 2


def test_split_statements_ignores_semicolon_in_block_comment():
    sql = "/* do this; then that */\nSELECT 1;"
    stmts = [s.strip() for s in split_statements(sql) if s.strip()]
    assert len(stmts) == 1


def test_split_statements_ignores_semicolon_in_multiline_block_comment():
    sql = "/* line one;\nline two; */\nSELECT 1;\nSELECT 2;"
    stmts = [s.strip() for s in split_statements(sql) if s.strip()]
    assert len(stmts) == 2


def test_strip_comments_removes_line_and_block_comments():
    statement = "SELECT 1 -- trailing comment\n/* block comment */, 2"
    clean = _strip_comments(statement)
    assert "comment" not in clean
    assert "SELECT 1" in clean
    assert ", 2" in clean


def test_strip_comments_preserves_string_literal_contents():
    statement = "INSERT INTO t (name) VALUES ('-- not a comment')"
    clean = _strip_comments(statement)
    assert "-- not a comment" in clean


def test_run_migrations_refuses_when_lock_already_held():
    try:
        with open(_LOCK_PATH, "w") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                run_migrations()
                raise AssertionError("expected SystemExit")
            except SystemExit as e:
                assert "concurrently" in str(e)
            fcntl.flock(lock_file, fcntl.LOCK_UN)
    finally:
        _LOCK_PATH.unlink(missing_ok=True)
