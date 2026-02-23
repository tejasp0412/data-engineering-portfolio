-- Data Quality Check: Business Rule
-- Every delivered transaction must have at least one service charge (line item)
-- Failure action: BLOCK — orders without items produce $0 revenue, distort ATV

select t1.order_id, t1.order_status
from {{ ref('stg_orders') }} t1
left join {{ ref('stg_order_items') }} t2 on t1.order_id = t2.order_id
where t2.order_id is null
and t1.order_status = 'delivered'