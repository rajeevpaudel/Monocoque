-- Incremental watermark: latest _ingested_at already materialized in this model.
{% macro incremental_watermark() %}
(SELECT coalesce(max(_ingested_at), toDateTime('1970-01-01')) FROM {{ this }})
{% endmacro %}

-- Predicate selecting every (season, round) whose Jolpica rows OR matching OpenF1 rows
-- arrived after the last run. Facts join both sources, so filtering on the Jolpica
-- timestamp alone misses OpenF1 data that lands later (NULL sectors / 'mismatch' forever).
-- Pair with incremental_strategy='delete+insert' so the whole round is rebuilt.
{% macro changed_rounds(season_col, round_col, jolpica_model, openf1_model='stg_openf1__laps', session_name=none) %}
({{ season_col }}, {{ round_col }}) IN (
    SELECT toUInt16(season), toUInt8(round)
    FROM {{ ref(jolpica_model) }}
    WHERE _ingested_at > {{ incremental_watermark() }}
    UNION DISTINCT
    SELECT toUInt16(sm.season), toUInt8(sm.round)
    FROM {{ ref(openf1_model) }} o
    INNER JOIN {{ ref('int_session_map') }} sm ON sm.session_key = o.session_key
    WHERE o._ingested_at > {{ incremental_watermark() }}
      AND sm.round > 0
      {% if session_name %}AND sm.session_name = '{{ session_name }}'{% endif %}
)
{% endmacro %}
