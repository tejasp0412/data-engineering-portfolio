{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw', 'raw_brl_usd_exchange_rate') }}
),
cleaned as (
    select cast(rate_date as date) as rate_date,
        upper(trim(currency_code)) as currency_code,
        cast(exchange_rate_to_usd as double) as exchange_rate_to_usd,
        
        --Metadata
        current_timestamp as transformed_at
    
    from source
    qualify row_number() over (partition by rate_date, currency_code order by transformed_at desc) = 1
)

select * from cleaned