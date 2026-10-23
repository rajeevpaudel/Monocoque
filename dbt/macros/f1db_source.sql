-- f1db uses its own ids ('lewis-hamilton', 'red-bull', 'melbourne'); Jolpica uses
-- 'hamilton', 'red_bull', 'albert_park'. f1db_source() wraps a raw_f1db table and rewrites
-- its id columns to Jolpica ids via the f1db_*_xref crosswalks, so seasons sourced from
-- either provider join on the same keys. Unmapped ids pass through unchanged.
{% macro _f1db_id_map(entity) -%}
{%- if entity == 'circuit' -%}
(SELECT mapFromArrays(groupArray(f1db_circuit_id), groupArray(jolpica_circuit_id)) FROM (
    SELECT f1db_circuit_id, argMin(jolpica_circuit_id, match_method != 'race') AS jolpica_circuit_id
    FROM {{ ref('f1db_circuit_xref') }} GROUP BY f1db_circuit_id))
{%- else -%}
(SELECT mapFromArrays(groupArray(f1db_{{ entity }}_id), groupArray(jolpica_{{ entity }}_id)) FROM {{ ref('f1db_' ~ entity ~ '_xref') }})
{%- endif -%}
{%- endmacro %}

{% macro f1db_source(table, driver_col=none, constructor_col=none, circuit_col=none) -%}
{%- set mapped = [] -%}
{%- for entity, col in [('driver', driver_col), ('constructor', constructor_col), ('circuit', circuit_col)] if col -%}
{%- do mapped.append("if(mapContains(" ~ _f1db_id_map(entity) ~ ", " ~ col ~ "), " ~ _f1db_id_map(entity) ~ "[" ~ col ~ "], " ~ col ~ ") AS " ~ col) -%}
{%- endfor -%}
(SELECT * REPLACE ({{ mapped | join(', ') }}) FROM {{ source('raw_f1db', table) }})
{%- endmacro %}
