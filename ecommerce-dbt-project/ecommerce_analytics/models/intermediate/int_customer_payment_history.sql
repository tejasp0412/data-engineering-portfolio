-- Customer-grain aggregation of payment behaviour across all transactions
-- Reframed: Account Holder payment profile for Utility Marketplace Platform

with enriched as (
    select * 
    from {{ ref('int_orders_enriched') }}
),

payment_types as (
    select t3.customer_unique_id,
        t1.payment_type,
        sum(t1.payment_value) as payment_value_by_type

    from {{ ref('stg_payments') }} t1
    inner join {{ ref('stg_orders') }} t2 on t1.order_id = t2.order_id
    inner join {{ ref('stg_customers') }} t3 on t2.customer_id = t3.customer_id

    group by 1,2
),

-- Identify dominant payment method per customer

dominant_payment as (

    select customer_unique_id,
        payment_type as dominant_payment_type
    from 
    (
        select customer_unique_id,
        payment_type,
        row_number() over(partition by customer_unique_id order by payment_value_by_type desc) as payment_type_rnk
    from payment_types
    ) sq
    where sq.payment_type_rnk = 1
),

customer_summary as (
    select customer_unique_id,

        -- Transaction Counts
        count(distinct order_id) as total_transactions,
        count(distinct case when is_delivered then order_id end) as successful_transactions,
        count(distinct case when is_failed_transaction then order_id end) as failed_transactions,

        -- Revenue / Volume (USD)
        sum(total_paid_usd) as total_payment_volume_usd,
        avg(total_paid_usd) as avg_transaction_value_usd,
        min(total_paid_usd) as min_transaction_value_usd,
        max(total_paid_usd) as max_transaction_value_usd,

        -- Payment Behaviour
        sum(case when is_installment_payment then 1 else 0 end) as installment_transaction_count,
        avg(max_installments) as avg_installments,

        -- Timing
        min(order_purchased_at) as first_transaction_date,
        max(order_purchased_at) as last_transaction_date,
        avg(days_to_approval) as avg_days_to_approval,

        -- Geography
        max(customer_state) as customer_state,
        max(customer_city) as customer_city,
        max(customer_zip_code_prefix) as customer_zip_code_prefix,

        -- Customer Tenure Determination
        {{ datediff('min(order_purchased_at)', 'max(order_purchased_at)', 'day') }} as customer_tenure_days

    from enriched

    group by 1
)

select t1.*,
    t2.dominant_payment_type,

    -- Calculated KPI fields
    round(t1.successful_transactions:: float/ nullif(t1.total_transactions, 0) * 100, 2) as transaction_success_rate_pct,
    round(t1.installment_transaction_count:: float / nullif(t1.total_transactions, 0) * 100, 2) as installment_rate_pct,

    -- Customer Segment (for dashboard filtering)
    case when t1.total_payment_volume_usd >= 500 then 'High Value'
        when t1.total_payment_volume_usd >= 100 then 'Mid Value'
        else 'Low Value'
    end as customer_segment

from customer_summary t1
left join dominant_payment t2 on t1.customer_unique_id = t2.customer_unique_id