{{ config(
    materialized='view'
) }}

with source as (
    select *
    from {{ source('raw', 'raw_orders') }}
),
cleaned as (
    select order_id, 
        customer_id, 
        -- Timestamp columns
        cast(order_purchase_timestamp as timestamp) as order_purchased_at,
        cast(order_approved_at as timestamp) as order_approved_at,
        cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date,
        cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date,
        cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date,

        -- Order Status
        upper(trim(order_status)) as order_status,

        --Metadata
        current_timestamp as transformed_at

    from source
)

select * from cleaned
