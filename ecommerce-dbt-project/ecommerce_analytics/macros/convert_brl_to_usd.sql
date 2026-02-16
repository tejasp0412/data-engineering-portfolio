{%- macro convert_brl_usd(column_name, exchange_rate) -%}
    round({{ column_name }} * {{ exchange_rate }}, 2)
{%- endmacro -%}