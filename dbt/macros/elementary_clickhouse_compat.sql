-- ClickHouse does not support session-scoped TEMPORARY TABLEs across HTTP connections.
-- Returning false makes Elementary use regular tables as intermediates instead,
-- matching the pattern used by Spark/Athena/Trino in Elementary's own codebase.
{% macro clickhouse__has_temp_table_support() %}
    {% do return(false) %}
{% endmacro %}

-- ClickHouse adapter override for Elementary's delete-and-insert pattern.
-- The default implementation wraps DELETE + INSERT in "begin transaction; ... commit;"
-- as a single string, which ClickHouse rejects (multi-statement HTTP requests are not
-- allowed). This override returns each statement as a separate list entry, matching
-- the pattern used by the Spark/Athena/Trino adapters in Elementary's own codebase.
{% macro clickhouse__get_delete_and_insert_queries(relation, insert_relation, delete_relation, delete_column_key) %}
    -- Skip DELETE for ClickHouse: the temp table referenced in the DELETE is created
    -- in a different HTTP session and is not visible to subsequent queries. Since
    -- Elementary's tables use ReplacingMergeTree, just INSERT — ClickHouse deduplicates
    -- on read (FINAL) and via background merges. Old entries are overwritten automatically.
    {% set queries = [] %}
    {% if insert_relation %}
        {% set insert_query %}
            insert into {{ relation }} select * from {{ insert_relation }}
        {% endset %}
        {% do queries.append(insert_query) %}
    {% endif %}
    {% do return(queries) %}
{% endmacro %}
