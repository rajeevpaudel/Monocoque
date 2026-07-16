from clickhouse.migrate import split_statements


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
