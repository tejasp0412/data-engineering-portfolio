{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw', 'raw_payments') }}
),
cleaned as (
    select --payment_id,
        order_id,
        payment_sequential as payment_sequence,
        
        -- Clean and Standardize Payment Type
        upper(trim(payment_type)) as payment_type,
        coalesce(payment_installments, 0) as payment_installments,
        round(payment_value, 2) as payment_value,

        --Metadata
        current_timestamp as transformed_at

    from source
)

select * from cleaned