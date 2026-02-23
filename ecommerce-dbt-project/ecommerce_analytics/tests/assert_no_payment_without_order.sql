-- Data Quality Check: Referential Integrity
-- Every payment record must have a matching order in stg_orders
-- Failure action: BLOCK pipeline — orphaned payments corrupt TPV calculations

select t1.order_id,
    count(*) as orphaned_payment_records
from {{ ref('stg_payments') }} t1
left join {{ ref('stg_orders') }} t2 on t1.order_id = t2.order_id
where t2.order_id is null
group by 1