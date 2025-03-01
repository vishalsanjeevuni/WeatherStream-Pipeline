{% macro celsius_to_fahrenheit(temp_col) %}
    ({{ temp_col }} * 9/5) + 32
{% endmacro %}

{% macro fahrenheit_to_celsius(temp_col) %}
    ({{ temp_col }} - 32) * 5/9
{% endmacro %}

{% macro kelvin_to_celsius(temp_col) %}
    {{ temp_col }} - 273.15
{% endmacro %}

{% macro kelvin_to_fahrenheit(temp_col) %}
    ({{ temp_col }} - 273.15) * 9/5 + 32
{% endmacro %}

{% macro round_temp(temp_col, decimals=1) %}
    ROUND({{ temp_col }}::NUMERIC, {{ decimals }})
{% endmacro %} 