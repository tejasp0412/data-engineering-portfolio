{{ config(
    materialized='table'
) }}

with orders as (
    select *
    from {{ ref('fct_orders') }}
    where is_failed_transaction = false
),

daily_metrics as (
    select order_date, 

        --Volume Metrics
        count(distinct order_id) as total_orders,
        count(distinct customer_key) as unique_customers,
        sum(total_items) as total_items_sold,

        --Revenue Metrics (USD for standardisation)
        sum(order_total_usd) as gross_revenue_usd,
        sum(total_freight_usd) as processing_fee_revenue_usd, 
        sum(total_item_price_usd) as service_revenue_usd,

        -- Revenue Metrics (BRL for local reporting)
        sum(order_total_brl) as gross_revenue_brl,

        --Averages
        round(avg(order_total_usd), 2) as avg_transaction_value_usd,
        round(avg(total_items), 2) as avg_items_per_transaction,

        -- Payment Behaviour
        sum(case when is_installment_payment then 1 else 0 end) as installment_transactions,
        round(avg(max_installments), 2) as avg_installments,

        --Delivery
        round(avg(days_to_deliver), 2) as avg_delivery_days,
        round(avg(days_to_approval), 2) as avg_approval_days,
        sum(case when is_late_delivery then 1 else 0 end) as late_deliveries,

        current_timestamp as updated_at  

    from orders
    group by 1
)

select * from daily_metrics
order by order_date 