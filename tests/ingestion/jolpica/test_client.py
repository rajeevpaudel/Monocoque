from unittest.mock import patch

from ingestion.jolpica.client import _paginate, paginate_all, raw


def _mrdata(items, total):
    return {"MRData": {"total": str(total), "Table": {"Items": items}}}


def test_paginate_yields_single_page_when_under_page_size():
    with patch("ingestion.jolpica.client.get_json") as mock_get:
        mock_get.return_value = _mrdata([{"a": 1}, {"a": 2}], total=2)
        pages = list(_paginate("path", "Table", "Items"))
    assert pages == [[{"a": 1}, {"a": 2}]]
    mock_get.assert_called_once_with(
        "https://api.jolpi.ca/ergast/f1/path.json", params={"limit": 100, "offset": 0}
    )


def test_paginate_stops_on_empty_page():
    with patch("ingestion.jolpica.client.get_json") as mock_get:
        mock_get.return_value = _mrdata([], total=0)
        pages = list(_paginate("path", "Table", "Items"))
    assert pages == []


def test_paginate_advances_offset_across_multiple_pages():
    page1 = _mrdata([{"a": i} for i in range(100)], total=150)
    page2 = _mrdata([{"a": i} for i in range(100, 150)], total=150)
    with patch("ingestion.jolpica.client.get_json", side_effect=[page1, page2]) as mock_get:
        pages = list(_paginate("path", "Table", "Items"))
    assert len(pages) == 2
    assert len(pages[0]) == 100
    assert len(pages[1]) == 50
    offsets = [c.kwargs["params"]["offset"] for c in mock_get.call_args_list]
    assert offsets == [0, 100]


def test_paginate_stops_when_offset_reaches_total_even_if_page_full():
    """If a page comes back full but offset+limit >= total, don't fetch another page."""
    page = _mrdata([{"a": i} for i in range(100)], total=100)
    with patch("ingestion.jolpica.client.get_json", return_value=page) as mock_get:
        pages = list(_paginate("path", "Table", "Items"))
    assert len(pages) == 1
    assert mock_get.call_count == 1


def test_paginate_all_flattens_pages():
    page1 = _mrdata([{"a": i} for i in range(100)], total=150)
    page2 = _mrdata([{"a": i} for i in range(100, 150)], total=150)
    with patch("ingestion.jolpica.client.get_json", side_effect=[page1, page2]):
        records = paginate_all("path", "Table", "Items")
    assert len(records) == 150
    assert records[0] == {"a": 0}
    assert records[-1] == {"a": 149}


def test_raw_serializes_without_ascii_escaping():
    assert raw({"name": "Perez"}) == '{"name": "Perez"}'
    assert raw({"name": "Räikkönen"}) == '{"name": "Räikkönen"}'
