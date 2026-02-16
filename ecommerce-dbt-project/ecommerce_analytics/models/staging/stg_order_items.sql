{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw', 'raw_order_items') }}
),
cleaned as (
    select order_id,
        order_item_id as line_item_number,
        product_id,
        seller_id,

        --Clean and Standardize
        cast(shipping_limit_date as timestamp) as shipping_limit_at,

        round(price, 2) as item_price,
        round(freight_value, 2) as freight_value,
        round(price + freight_value, 2) as item_total,

        --Metadata
        current_timestamp as transformed_at

    from source
)

select * from cleaned