{{ config(
    materialized='view'
) }}

with orders as (
    select * 
    from {{ ref('stg_orders') }}
),

customers as (
    select *
    from {{ ref('stg_customers') }}
),

products as (
    select * 
    from {{ ref('stg_products') }}
),

order_items as (
    select order_id,
        count(*) as total_items,
        sum(item_price) as total_item_price,
        sum(freight_value) as total_freight,
        sum(item_total) as order_total
    from {{ ref('stg_order_items') }}
    group by 1
),

payments as (
    select order_id, 
        sum(payment_value) as total_paid,
        count(distinct payment_type) as payment_method_used
    from {{ ref('stg_payments') }}
    group by 1
),

exchange_rate as (
    select *
    from {{ ref('stg_brl_usd_exchange_rate') }}
),

enriched as (
    select t1.order_id, 
        t1.customer_id, 
        t2.customer_unique_id,
        t2.customer_city,
        t2.customer_state,

        t1.order_status,
        t1.order_purchased_at,
        t1.order_approved_at,
        --t1.order_shipped_at,
        t1.order_delivered_carrier_date,
        t1.order_delivered_customer_date,
        t1.order_estimated_delivery_date,

        --Delivery Performance Metrics
        {{ datediff('t1.order_purchased_at', 't1.order_delivered_customer_date', 'day') }} as days_to_deliver,
        --{{ datediff('t1.order_purchased_at', 't1.order_shipped_at', 'day') }} as days_to_ship,

        --Order Details
        coalesce(t3.total_items, 0) as total_items,
        coalesce(t3.total_item_price, 0) as total_item_price,
        {{ convert_brl_usd('t3.total_item_price', 't5.exchange_rate_to_usd') }} as total_item_price_usd,
        coalesce(t3.total_freight, 0) as total_freight,
        {{ convert_brl_usd('t3.total_freight', 't5.exchange_rate_to_usd') }} as total_freight_usd,
        coalesce(t3.order_total, 0) as order_total,
        {{ convert_brl_usd('t3.order_total', 't5.exchange_rate_to_usd') }} as order_total_usd,

        --Payment Details
        coalesce(t4.total_paid, 0) as total_paid,
        coalesce(t4.payment_method_used, 0) as payment_method_used


    from orders t1
    left join customers t2 on t1.customer_id = t2.customer_id
    left join order_items t3 on t1.order_id = t3.order_id
    left join payments t4 on t1.order_id = t4.order_id
    left join exchange_rate t5 on date_trunc('day', t1.order_purchased_at) = t5.rate_date
)

select * from enriched