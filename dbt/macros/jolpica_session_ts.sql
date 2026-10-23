-- Jolpica schedule JSON: {"<Session>": {"date": "YYYY-MM-DD", "time": "HH:MM:SSZ"}} → DateTime (UTC), NULL if absent.
{% macro jolpica_session_ts(key) -%}
parseDateTimeBestEffortOrNull(concat(
    JSONExtractString(_raw_json, '{{ key }}', 'date'), ' ',
    replaceOne(JSONExtractString(_raw_json, '{{ key }}', 'time'), 'Z', '')
))
{%- endmacro %}
