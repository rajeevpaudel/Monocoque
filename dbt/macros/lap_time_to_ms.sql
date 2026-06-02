{% macro lap_time_to_ms(col) %}
(multiIf(
    {{ col }} IS NULL,
    NULL,
    position({{ col }}, ':') > 0,
    toInt32OrZero(substring({{ col }}, 1, position({{ col }}, ':') - 1)) * 60000
    + toInt32OrZero(substring({{ col }}, position({{ col }}, ':') + 1,
                              position({{ col }}, '.') - position({{ col }}, ':') - 1)) * 1000
    + toInt32OrZero(leftPad(substring({{ col }}, position({{ col }}, '.') + 1), 3, '0')),
    toInt32OrZero(substring({{ col }}, 1, position({{ col }}, '.') - 1)) * 1000
    + toInt32OrZero(leftPad(substring({{ col }}, position({{ col }}, '.') + 1), 3, '0'))
))
{% endmacro %}
