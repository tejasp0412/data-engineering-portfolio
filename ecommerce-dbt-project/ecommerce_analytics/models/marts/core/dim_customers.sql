{{ config(
    materialized='table'
) }}

with customers as (
    select *
    from {{ ref('stg_customers') }}
),
orders as (
    select *
    from {{ ref('int_orders_enriched') }}
),
customer_metrics as (
    select customer_unique_id,
        count(distinct order_id) as total_orders,
        sum(order_total) as lifetime_value,
        avg(order_total) as avg_order_value,
        min(order_purchased_at) as first_order_at,
        max(order_purchased_at) as last_order_at,
        avg(days_to_deliver) as avg_delivery_days
    from orders
    group by 1
),
final as (
    select {{ generate_surrogate_key(['t1.customer_unique_id']) }} as customer_key,
        t1.customer_unique_id,
        t1.customer_city,
        t1.customer_state,
        t1.customer_zip_code_prefix,

        coalesce(t2.total_orders, 0) as total_orders,
        coalesce(t2.lifetime_value, 0) as lifetime_value,
        coalesce(t2.avg_order_value, 0) as avg_order_value,

        t2.first_order_at,
        t2.last_order_at,
        coalesce(t2.avg_delivery_days, 0) as avg_delivery_days,

        --Customer Segmentation Logic
        case when t2.total_orders >= 10 and t2.lifetime_value >= 1000 then 'VIP'
             when t2.total_orders >= 5 and t2.lifetime_value >= 500 then 'LOYAL'
             when t2.total_orders >= 1 then 'NEW'
             else 'PROSPECT' end as customer_segment,

        current_timestamp as updated_at

    from customers t1
    left join customer_metrics t2 on t1.customer_unique_id = t2.customer_unique_id

    --Deduplicate to unique customers
    qualify row_number() over(partition by t1.customer_unique_id order by t1.customer_id) = 1
)

select * from final