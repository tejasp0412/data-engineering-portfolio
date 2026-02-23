{{ config(
    materialized='table',
    schema='finance'
) }}

-- Grain: One row per payment transaction (order)
-- Purpose: Primary KPI-serving model for Payment Operations reporting
-- Reframed: Utility Marketplace Payment Processing & Revenue Operations

with base as (
    select *
    from {{ ref('int_orders_enriched') }}
),

-- Monthly aggregation window for MoM comparisons
monthly_context as (
    select date_trunc('month', order_purchased_at) as transaction_month,
        sum(total_paid_usd) as monthly_tpv_usd,
        count(distinct order_id) as monthly_transaction_count,
        count(case when is_delivered then order_id end) as monthly_successful_count,
    from base 
    group by 1
),
final as (
    select 
        -- Keys
        t1.order_id,
        t1.customer_unique_id,

        -- Geography
        t1.customer_city,
        t1.customer_state,
        t1.customer_zip_code_prefix,

        -- Timestamps
        t1.order_purchased_at,
        t1.order_approved_at,
        t1.order_delivered_customer_date,
        date_trunc('day',   t1.order_purchased_at)   as transaction_date,
        date_trunc('month', t1.order_purchased_at)   as transaction_month,
        date_trunc('year',  t1.order_purchased_at)   as transaction_year,

        -- Transaction Status
        t1.order_status,
        t1.primary_payment_type,
        t1.max_installments,
        t1.is_installment_payment,
        t1.is_delivered,
        t1.is_failed_transaction,

        -- KPIs

        -- KPI 1: Total Payment Volume (TPV) per transaction in USD
        -- Definition: Sum of payment value received per transaction
        t1.total_paid_usd as transaction_value_usd,
        t1.total_paid_brl as transaction_value_brl,

        -- KPI 2: Processing Fee (Freight as % of transaction)
        -- Definition: freight_value / order_total * 100
        t1.total_freight_usd as processing_fee_usd,
        round(t1.total_freight_usd/nullif( t1.total_paid_usd,0) * 100, 2) as processing_fee_pct,

        -- KPI 3: Service Revenue (net of processing fees)
        -- Definition: total_item_price_usd (excludes freight)
        coalesce(t1.total_item_price_usd, 0) as service_revenue_usd,

        -- KPI 4: Days to Payment Approval
        -- Definition: order_approved_at - order_purchased_at in days
        -- Flag for SLA breach (>2 days = breach)
        t1.days_to_approval,
        case when t1.days_to_approval > 2 then true else false end as is_approval_sla_breached,

        -- KPI 5: Days to Delivery
        -- Definition: order_delivered_customer_date - order_purchased_at
        t1.days_to_deliver,
        case when t1.days_to_deliver >  {{ datediff('t1.order_purchased_at', 't1.order_estimated_delivery_date', 'day') }} then true else false end as is_late_delivery,

        -- KPI 6: Payment Installment Rate (transaction level flag)
        -- Definition: transactions with installments > 1
        t1.max_installments,

        -- KPI 7: Transaction Success (delivered = successful)
        -- Numerator for success rate aggregations
        case when t1.is_delivered then 1 else 0 end as is_success_int,
        case when t1.is_failed_transaction then 1 else 0 end as is_failure_int,

        -- Window KPIs: require monthly_context join
        -- KPI 8: Transaction's share of Monthly TPV
        -- Definition: transaction_value / monthly_tpv * 100
        round(t1.total_paid_usd / nullif(t2.monthly_tpv_usd, 0) * 100, 2) as pct_of_monthly_tpv,

        -- KPI 9: Monthly TPV (denormalised for dashboard convenience)
        t2.monthly_tpv_usd,
        t2.monthly_transaction_count,

        -- KPI 10: Monthly Transaction Success Rate
        -- Definition: successful_transactions / total_transactions * 100
        round(t2.monthly_successful_count::float/nullif(t2.monthly_transaction_count, 0) * 100, 2) as monthly_success_rate_pct,

        current_timestamp as updated_at

    from base t1
    left join monthly_context t2 on date_trunc('month', t1.order_purchased_at) = t2.transaction_month
)

select * from final