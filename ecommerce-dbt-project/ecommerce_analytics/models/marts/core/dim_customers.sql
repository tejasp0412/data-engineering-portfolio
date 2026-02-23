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
        count(distinct order_id) as total_transactions,
        sum(order_total_usd) as lifetime_value_usd,
        avg(order_total_usd) as avg_transaction_value_usd,
        min(order_purchased_at) as first_transaction_at,
        max(order_purchased_at) as last_transaction_at,
        avg(days_to_deliver) as avg_delivery_days,
        avg(days_to_approval) as avg_approval_days,
        sum(case when is_installment_payment then 1 else 0 end) as installment_transaction_count,
        sum(case when is_failed_transaction then 1 else 0 end) as failed_transaction_count
    from orders
    group by 1
),
final as (
    select {{ generate_surrogate_key(['t1.customer_unique_id']) }} as customer_key,
        t1.customer_unique_id,
        t1.customer_city,
        t1.customer_state,
        t1.customer_zip_code_prefix,

         -- Transaction Metrics
        coalesce(t2.total_transactions, 0) as total_transactions,
        coalesce(t2.lifetime_value_usd, 0) as lifetime_value_usd,
        coalesce(t2.avg_transaction_value_usd, 0) as avg_transaction_value_usd,
        coalesce(t2.avg_delivery_days, 0) as avg_delivery_days,
        coalesce(t2.avg_approval_days, 0) as avg_approval_days,
        coalesce(t2.installment_transaction_count,0) as installment_transaction_count,
        coalesce(t2.failed_transaction_count, 0) as failed_transaction_count,

        -- Timestamps
        t2.first_transaction_at,
        t2.last_transaction_at,
        
        -- Tenure in days
        {{ datediff('t2.first_transaction_at', 'current_timestamp', 'day') }} as customer_tenure_days,

        -- Customer Segmentation (reframed for utility platform)
        case
            when t2.total_transactions >= 10 and t2.lifetime_value_usd >= 1000  then 'VIP'
            when t2.total_transactions >= 5  and t2.lifetime_value_usd >= 500   then 'LOYAL'
            when t2.total_transactions >= 1 then 'ACTIVE'
            else 'PROSPECT'
        end as customer_segment,

        current_timestamp as updated_at

    from customers t1
    left join customer_metrics t2 on t1.customer_unique_id = t2.customer_unique_id

    -- Deduplicate to one row per unique account holder
    qualify row_number() over(partition by t1.customer_unique_id order by t1.customer_id) = 1
)

select * from final