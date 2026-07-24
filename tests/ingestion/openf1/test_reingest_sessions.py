def test_module_imports():
    """Guards against a repeat of the dead-import bug where reingest_sessions.py
    imported names (_load_checkpoint/_save_checkpoint) that only ever existed
    in ingestion.fastf1.backfill, not ingestion.openf1.backfill — the module
    failed at import time before argparse even ran."""
    import ingestion.openf1.reingest_sessions  # noqa: F401
