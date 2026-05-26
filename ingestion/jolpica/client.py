"""Jolpica API client with limit/offset pagination over the MRData envelope."""

import json
from typing import Any, Generator

from ingestion.shared.http import get_json

BASE_URL = "https://api.jolpi.ca/ergast/f1"
PAGE_SIZE = 100


def _paginate(path: str, data_key: str, inner_key: str) -> Generator[list[dict], None, None]:
    """Yield pages of results for a given Jolpica endpoint."""
    offset = 0
    while True:
        url = f"{BASE_URL}/{path}.json"
        data = get_json(url, params={"limit": PAGE_SIZE, "offset": offset})
        table = data["MRData"][data_key][inner_key]
        if not table:
            break
        yield table
        total = int(data["MRData"]["total"])
        offset += PAGE_SIZE
        if offset >= total:
            break


def paginate_all(path: str, data_key: str, inner_key: str) -> list[dict]:
    """Return all records across all pages for a Jolpica endpoint."""
    records = []
    for page in _paginate(path, data_key, inner_key):
        records.extend(page)
    return records


def raw(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)
