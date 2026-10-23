"""Exceptions shared by ingestion entry points."""


class IngestionIncomplete(RuntimeError):
    """Raised (or turned into a non-zero exit) when some tables/sessions failed to load.

    Successful parts are kept; failed ones are retried on the next run (ingestion_log /
    checkpoint). This exists so Airflow tasks and CLI runs don't report success on a
    partial load.
    """

    def __init__(self, what: str, failed: list) -> None:
        self.failed = failed
        super().__init__(f"{what}: {len(failed)} failed — {failed[:20]}")
