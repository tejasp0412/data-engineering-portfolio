{{ config(
    materialized='table',
    schema='finance'
) }}

-- Grain: One row per unique account holder (customer_unique_id)
-- Purpose: Customer-level KPIs for CLV, retention, and risk reporting
-- Reframed: Account Holder Revenue & Risk Analytics

with customer_history as (
    select *
    from {{ ref('int_customer_payment_history') }}
),

-- Percentile context for CLV benchmarking
clv_context as (
    select percentile_cont(0.25) within group (order by total_payment_volume_usd) as clv_p25,
        percentile_cont(0.50) within group (order by total_payment_volume_usd) as clv_p50,
        percentile_cont(0.75) within group (order by total_payment_volume_usd) as clv_p75,
        percentile_cont(0.90) within group (order by total_payment_volume_usd) as clv_p90,
        avg(total_payment_volume_usd) as clv_avg
    from customer_history
),

-- Cohort: month of first transaction (for retention analysis)
cohort_context as (
    select date_trunc('month', first_transaction_date) as cohort_month,
        count(distinct customer_unique_id) as cohort_size
    from customer_history
    group by 1
),

final as (
    select 
        -- Keys
        t1.customer_unique_id,
        t1.customer_city,
        t1.customer_state,
        t1.customer_zip_code_prefix,

        -- KPI 7: Customer Lifetime Value (CLV)
        -- Definition: SUM of all payment values in USD per customer
        t1.total_payment_volume_usd as clv_usd,

        -- CLV Percentile Tier (where does this customer rank?)
        case when t1.total_payment_volume_usd >= t2.clv_p90 then 'Top 10%'
             when t1.total_payment_volume_usd >= t2.clv_p75 then 'Top 25%'
             when t1.total_payment_volume_usd >= t2.clv_p50 then 'Above Median'
             when t1.total_payment_volume_usd >= t2.clv_p25 then 'Below Median'
             else 'Bottom 25%'
        end as clv_percentile_tier,

        -- KPI 8: Average Transaction Value (ATV)
        -- Definition: CLV / total_transactions
        t1.avg_transaction_value_usd as atv_usd,

        -- KPI 9: Transaction Success Rate per customer
        -- Definition: successful_transactions / total_transactions
        t1.total_transactions,
        t1.successful_transactions,
        t1.failed_transactions,
        t1.transaction_success_rate_pct,

        -- KPI 10: Payment Installment Rate per customer
        -- Definition: installment_transactions / total_transactions
        -- Risk signal: customers with high installment rate
        t1.installment_transaction_count,
        t1.installment_rate_pct,
        -- Risk Tier based on installment behaviour
        case when t1.installment_rate_pct >= 75 then 'High Risk'
            when t1.installment_rate_pct >= 40 then 'Medium Risk'
        else 'Low Risk'
        end as payment_risk_tier,

         -- KPI 11: Days to Approval (avg per customer)
         round(t1.avg_days_to_approval, 2) as avg_days_to_approval,

         -- KPI 12: Customer Tenure & Retention signals
         -- Definition: days between first and last transaction
         -- Repeat buyer = tenure_days > 0 and total_transactions > 1
         t1.customer_tenure_days,
         t1.first_transaction_date,
         t1.last_transaction_date,
         case when t1.total_transactions > 1 then true else false end as is_repeat_payer,

         -- Cohort month for retention analysis
         date_trunc('month', t1.first_transaction_date) as cohort_month, 
         t3.cohort_size,

         -- Payment Behaviour
         t1.dominant_payment_type,
         t1.avg_installments,

         -- Segment
         t1.customer_segment,

         current_timestamp as updated_at

        from customer_history t1
        cross join clv_context t2
        left join cohort_context t3 on date_trunc('month', t1.first_transaction_date) = t3.cohort_month
)

select * from final