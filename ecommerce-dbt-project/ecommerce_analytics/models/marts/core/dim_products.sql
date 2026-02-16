{{ config(
    materialized='table'
) }}

with products as (
    select *
    from {{ ref('stg_products') }}
),
order_items as (
    select *
    from {{ ref('stg_order_items') }}
),
product_metrics as (
    select product_id, 
        count(distinct order_id) as times_ordered,
        sum(item_price) as total_revenue
    from order_items
    group by 1
),
final as (
    select {{ generate_surrogate_key(['t1.product_id']) }} as product_key,
        t1.product_id,
        t1.product_category_name,
        t1.product_weight_grams,
        t1.product_length_cm,
        t1.product_height_cm,
        t1.product_width_cm,

        --Calculated Product Volume
        round(t1.product_length_cm * t1.product_height_cm * t1.product_width_cm, 2) as product_volume_cm3,

        --Product Performance Metrics
        coalesce(t2.times_ordered, 0) as times_ordered,
        coalesce(t2.total_revenue, 0) as total_revenue,

        current_timestamp as updated_at
    
    from products t1
    left join product_metrics t2 on t1.product_id = t2.product_id
)

select * from final