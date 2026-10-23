-- Accent-, case- and punctuation-insensitive name key: 'Pérez Jr.' → 'perezjr'.
{% macro normalize_name(expr) -%}
replaceRegexpAll(lowerUTF8(replaceRegexpAll(normalizeUTF8NFD({{ expr }}), '\\p{Mn}', '')), '[^a-z0-9]', '')
{%- endmacro %}
