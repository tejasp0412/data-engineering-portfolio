{%- macro datediff(start_date, end_date, datepart) -%}
    {%- if target.type == 'duckdb' -%}
        date_diff('{{ datepart }}', {{ start_date }}, {{ end_date }})
    
    {%- elif target.type == 'snowflake' -%}
        datediff( {{ datepart }}, {{ start_date }}, {{ end_date }})
    
    {%- elif target.type == 'bigquery' -%}
        date_diff({{ end_date }}, {{ start_date }}, {{ datepart }} )
    
    {%- else -%}
        datediff( {{ datepart }}, {{ start_date }}, {{ end_date }})
    
    {%- endif -%}
{%- endmacro -%}