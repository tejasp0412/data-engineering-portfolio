{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw', 'raw_customers') }}
),
cleaned as (
    select customer_id,
        customer_unique_id,

        --Clean and Standardize
        upper(trim(customer_city)) as customer_city,
        upper(trim(customer_state)) as customer_state,
        customer_zip_code_prefix as customer_zip_code_prefix,

        --Metadata
        current_timestamp as transformed_at

    from source
)

select * from cleaned