{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with orders as (
    select *
    from {{ ref('int_orders_enriched') }}

    {%- if is_incremental() -%}
        where order_purchased_at > (select max(order_purchased_at) from {{ this }})
    {%- endif -%}
),

final as (
    select 
        --Keys
        order_id, 
        {{ generate_surrogate_key(['customer_unique_id']) }} as customer_key,
        customer_unique_id,

        --Dimensions
        customer_city,
        customer_state,
        order_status,
        primary_payment_type,

        -- Timestamps
        order_purchased_at,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,

        --Date dimensions for easy filtering
        date_trunc('day', order_purchased_at) as order_date,
        date_trunc('month', order_purchased_at) as order_month,
        date_trunc('year', order_purchased_at) as order_year,

        -- Transaction Financials (BRL)
        total_items,
        total_item_price_brl,
        total_freight_brl,
        order_total_brl,
        total_paid_brl,

        -- Transaction Financials (USD)
        total_item_price_usd,
        total_freight_usd,
        order_total_usd,
        total_paid_usd,

        -- Payment Details
        payment_method_used,
        max_installments,
        is_installment_payment,

        -- Delivery Metrics
        days_to_deliver,
        days_to_approval,

        -- Flags (use lowercase to match stg_orders output)
        is_delivered,
        is_failed_transaction,
        case 
            when days_to_deliver > {{ datediff('order_purchased_at', 'order_estimated_delivery_date', 'day') }} 
            then true else false 
        end as is_late_delivery,

        current_timestamp as updated_at
    
    from orders 

)

select * from final