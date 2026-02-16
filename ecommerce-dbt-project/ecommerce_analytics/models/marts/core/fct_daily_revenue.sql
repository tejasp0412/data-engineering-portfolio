{{ config(
    materialized='table'
) }}

with orders as (
    select *
    from {{ ref('fct_orders') }}
    where is_canceled = false
),

daily_metrics as (
    select order_date, 

        --Volume Metrics
        count(distinct order_id) as total_orders,
        count(distinct customer_key) as unique_customers,
        sum(total_items) as total_items_sold,

        --Revenue Metrics
        sum(order_total) as gross_revenue,
        sum(total_freight) as freight_revenue, 
        sum(total_item_price) as product_revenue,

        --Averages
        round(avg(order_total), 2) as avg_order_value,
        round(avg(total_items), 2) as avg_items_per_order,

        --Delivery
        round(avg(days_to_deliver), 2) as avg_delivery_days,
        sum(case when is_late_delivery then 1 else 0 end) as late_deliveries,

        current_timestamp as updated_at  

    from orders
    group by 1
)

select * from daily_metrics
order by order_date 